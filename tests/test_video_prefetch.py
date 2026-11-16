"""Bounded source-only lookahead; synthetic data and no Google requests."""
from contextlib import contextmanager
import io,json,sys,tempfile,threading,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch,MagicMock

from converter import digest
from workflow import VideoPrefetch,SourceUnavailableError
from video_migration import Videos
from test_selection import fixture
import test_reliability

class Prefetch(unittest.TestCase):
    def window(self,rows,prepare,stop=None,**kw):
        return VideoPrefetch(rows,prepare,lambda row:True,lambda row:10,
                             stop or threading.Event(),Path(tempfile.gettempdir()),**kw)

    def test_downloads_overlap_foreground_work_with_bounded_window_and_workers(self):
        rows=[[str(i)] for i in range(9)];release=threading.Event();started=threading.Event();lock=threading.Lock()
        active=peak=0;calls=[]
        def prepare(row):
            nonlocal active,peak
            with lock:
                active+=1;peak=max(peak,active);calls.append(row[0])
                if active==2:started.set()
            if not release.wait(3):raise AssertionError('Test did not release download')
            with lock:active-=1
            return row[0]
        with self.window(rows,prepare,size=4) as lookahead:
            self.assertTrue(started.wait(2));self.assertEqual(len(lookahead.futures),4)
            self.assertEqual(len(calls),2)
            release.set()
            for row in rows:
                self.assertEqual(lookahead.prepare(row),row[0])
                self.assertLessEqual(len(lookahead.futures),4)
        self.assertLessEqual(peak,2);self.assertCountEqual(calls,[r[0] for r in rows])

    def test_oversized_source_falls_back_to_foreground_without_prefetch(self):
        prepare=Mock(return_value='ready')
        with self.window([['big']],prepare,size=2,max_bytes=1) as lookahead:
            prepare.assert_not_called();self.assertFalse(lookahead.futures)
            self.assertEqual(lookahead.prepare(['big']),'ready')
        prepare.assert_called_once_with(['big'])

    def test_low_disk_space_does_not_start_lookahead(self):
        prepare=Mock()
        with patch('shutil.disk_usage',return_value=SimpleNamespace(free=100)):
            with self.window([['source']],prepare,size=2) as lookahead:
                self.assertFalse(lookahead.futures)
        prepare.assert_not_called()

    def test_unavailable_source_error_is_observed_at_foreground_admission(self):
        stop=threading.Event()
        def prepare(row):
            if row[0]=='missing':raise SourceUnavailableError('Original source is unavailable')
            return 'good'
        with self.window([['missing'],['good']],prepare,stop,size=2) as lookahead:
            with self.assertRaises(SourceUnavailableError):lookahead.prepare(['missing'])
            self.assertFalse(stop.is_set());self.assertEqual(lookahead.prepare(['good']),'good')

    def test_fatal_prefetch_error_stops_admission_and_is_not_swallowed(self):
        stop=threading.Event()
        def prepare(row):raise RuntimeError('Video account mismatch')
        with self.assertRaisesRegex(RuntimeError,'account mismatch'):
            with self.window([['bad'],['later']],prepare,stop,size=1) as lookahead:
                self.assertTrue(stop.wait(2));lookahead.check()
        self.assertTrue(stop.is_set())

    def test_pending_downloads_cancel_on_foreground_failure(self):
        release=threading.Event();two_started=threading.Event();calls=[];lock=threading.Lock()
        def prepare(row):
            with lock:
                calls.append(row[0])
                if len(calls)==2:two_started.set()
            if not release.wait(3):raise AssertionError('Test did not release download')
        with self.assertRaisesRegex(RuntimeError,'foreground failure'):
            with self.window([[str(i)] for i in range(4)],prepare,size=4) as lookahead:
                self.assertTrue(two_started.wait(2))
                shutdown=lookahead.pool.shutdown
                def finish(**kw):release.set();return shutdown(**kw)
                lookahead.pool.shutdown=finish
                raise RuntimeError('foreground failure')
        self.assertEqual(len(calls),2)

    def test_invalid_window_sizes_are_rejected(self):
        for size in (0,5,True,1.5):
            with self.subTest(size=size),self.assertRaises(ValueError):self.window([],Mock(),size=size)

    def test_source_preparation_never_encodes_uploads_or_queues_replacements(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);work=root/'work-video';work.mkdir();row,ext=fixture(filename='clip.mov',video=True)
            payload=root/'export.mov';payload.write_bytes(b'synthetic original');ext[0][5]=payload.stat().st_size
            worker=Videos.__new__(Videos);worker.account='example';worker.cat={'items':{'old':ext}}
            worker.selection=SimpleNamespace(delete_mode='permanent');worker.api=SimpleNamespace(info=Mock(return_value=[['old']]))
            worker.waiting=[];worker.lock=threading.Lock()
            @contextmanager
            def original(*args):yield [('clip.mov',payload)]
            with patch('video_migration.WORK',work),patch('video_migration.original_files',original),patch('video_migration.encode') as encode,patch('video_migration.upload') as upload:
                state=worker.prepare_source(row)
                self.assertEqual(state['stage'],'downloaded');self.assertEqual(Path(state['source']).read_bytes(),payload.read_bytes())
                self.assertEqual(state['original_sha256'],digest(Path(state['source'])))
                state['replacement_key']='candidate';(work/'old/state.json').write_text(json.dumps(state))
                worker.prepare_source(row)
                self.assertFalse(worker.waiting);encode.assert_not_called();upload.assert_not_called()

    def test_foreground_rechecks_prefetched_backup_hash(self):
        with tempfile.TemporaryDirectory() as td:
            source=Path(td)/'original.mov';source.write_bytes(b'original')
            state={'source':str(source),'output':str(Path(td)/'output.mp4'),'original_sha256':digest(source),'original_ext':[[None]*20]}
            source.write_bytes(b'changed');worker=Videos.__new__(Videos);worker.prefetch=SimpleNamespace(prepare=Mock(return_value=state))
            with patch('video_migration.encode') as encode,patch('video_migration.upload') as upload,self.assertRaisesRegex(RuntimeError,'backup changed'):
                worker.process(['old'])
            encode.assert_not_called();upload.assert_not_called()

    def test_prefetch_receives_only_the_filtered_and_limited_video_selection(self):
        cli=test_reliability.cli_module()
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);pairs=[fixture(key='outside',video=True,captured='2024-01-01T00:00:00+00:00'),fixture(key='video',video=True,captured='2025-01-01T00:00:00+00:00'),fixture(key='image',captured='2025-01-01T00:00:00+00:00'),fixture(key='limited',video=True,captured='2025-01-01T00:00:00+00:00')]
            inv={'account':'example','complete':True,'items':[r for r,e in pairs]};cat={'account':'example','items':{r[0]:e for r,e in pairs}}
            (root/'inventory-api.json').write_text(json.dumps(inv));(root/'catalog-api.json').write_text(json.dumps(cat))
            images=Mock(inventory=inv,catalog=cat);videos=Mock();images.run.return_value=videos.run.return_value=([],[])
            ctx=MagicMock();ctx.__enter__.return_value=SimpleNamespace(check=lambda:None)
            with patch.object(sys,'argv',['cli','--data-dir',td,'run','--year','2025','--limit','2','--apply','--video-prefetch','2']),patch('migration.Migration',return_value=images),patch('video_migration.Videos',return_value=videos),patch('workflow.VideoPrefetch',return_value=ctx) as prefetch,patch.object(cli,'complete_workflow'):
                cli.main()
            self.assertEqual([r[0] for r in prefetch.call_args.args[0]],['video'])
            self.assertEqual(prefetch.call_args.kwargs['size'],2)


    def test_completed_pending_and_committing_sources_are_not_prefetched(self):
        with tempfile.TemporaryDirectory() as td:
            work=Path(td);folder=work/'old';folder.mkdir()
            worker=Videos.__new__(Videos);worker.account='example'
            for extra in ({'stage':'trashed'},{'stage':'already_efficient'},
                          {'stage':'committing'},{'replacement_key':'candidate'}):
                with self.subTest(extra=extra):
                    (folder/'state.json').write_text(json.dumps(dict(original_key='old',account='example',**extra)))
                    with patch('video_migration.WORK',work):self.assertFalse(worker.needs_download(['old']))

    def test_cached_foreign_source_metadata_fails_before_any_download(self):
        with tempfile.TemporaryDirectory() as td:
            work=Path(td);folder=work/'old';folder.mkdir()
            (folder/'state.json').write_text(json.dumps(dict(original_key='old',account='example',original_ext=[['foreign']])))
            worker=Videos.__new__(Videos);worker.account='example'
            with patch('video_migration.WORK',work),patch('video_migration.original_files') as download,self.assertRaisesRegex(RuntimeError,'identity mismatch'):
                worker.prepare_source(['old'])
            download.assert_not_called()
