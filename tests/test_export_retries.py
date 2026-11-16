import io
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from photos_api import original_files

URL='https://video-downloads.googleusercontent.com/synthetic-export'
class Response(io.BytesIO):
    status=200

class ExportRetries(unittest.TestCase):
    def api(self):return SimpleNamespace(rpc=Mock(return_value=[URL]))

    def test_server_failure_refreshes_url_and_retries_only_download(self):
        api=self.api()
        with patch('urllib.request.urlopen',side_effect=[HTTPError(URL,500,'temporary',None,None),Response(b'complete')]),patch('photos_api.time.sleep'):
            with original_files(api,['source']) as files:
                path=files[0][1];self.assertEqual(path.read_bytes(),b'complete')
        self.assertFalse(path.exists());self.assertEqual(api.rpc.call_count,2)
        self.assertTrue(all(c.args==('pLFTfd',[['source'],None,None]) for c in api.rpc.call_args_list))

    def test_partial_body_is_discarded_before_retry(self):
        class Partial(Response):
            def read(self,size=-1):
                if self.tell():raise TimeoutError('synthetic timeout')
                return super().read(3)
        partial=Partial(b'partial');api=self.api()
        with patch('urllib.request.urlopen',side_effect=[partial,Response(b'complete')]),patch('photos_api.time.sleep'):
            with original_files(api,['source']) as files:self.assertEqual(files[0][1].read_bytes(),b'complete')
        self.assertTrue(partial.closed);self.assertEqual(api.rpc.call_count,2)

    def test_attempts_are_bounded(self):
        api=self.api()
        with patch('urllib.request.urlopen',side_effect=HTTPError(URL,503,'temporary',None,None)),patch('photos_api.time.sleep') as pause,self.assertRaises(HTTPError):
            with original_files(api,['source']):self.fail('No partial export may be yielded')
        self.assertEqual(api.rpc.call_count,4);self.assertEqual(pause.call_count,3)

    def test_permission_error_is_not_retried(self):
        api=self.api()
        with patch('urllib.request.urlopen',side_effect=HTTPError(URL,403,'denied',None,None)),patch('photos_api.time.sleep') as pause,self.assertRaises(HTTPError):
            with original_files(api,['source']):pass
        self.assertEqual(api.rpc.call_count,1);pause.assert_not_called()

    def test_consumer_checksum_or_timeout_failure_is_not_retried(self):
        for error in (RuntimeError('checksum mismatch'),TimeoutError('consumer failure')):
            with self.subTest(error=type(error).__name__):
                api=self.api()
                with patch('urllib.request.urlopen',return_value=Response(b'complete')),patch('photos_api.time.sleep') as pause,self.assertRaises(type(error)):
                    with original_files(api,['source']) as files:
                        path=files[0][1];raise error
                self.assertFalse(path.exists());self.assertEqual(api.rpc.call_count,1);pause.assert_not_called()
