import unittest
from video_codec import audio_maps
class AudioMapping(unittest.TestCase):
    def test_default_aac_avoids_auxiliary_apac_and_keeps_other_audio(self):
        probe={'streams':[
            {'index':0,'codec_type':'video','codec_name':'h264'},
            {'index':1,'codec_type':'audio','codec_name':'aac','disposition':{'default':1}},
            {'index':2,'codec_type':'audio','codec_name':'apple_apac','disposition':{'default':0}},
            {'index':3,'codec_type':'audio','codec_name':'aac','disposition':{'default':0}},
        ]}
        self.assertEqual(audio_maps(probe),['0:1','0:3'])
    def test_no_compatible_default_does_not_silently_drop_audio(self):
        for streams in (
            [],
            [{'index':1,'codec_type':'audio','codec_name':'aac'}],
            [{'index':1,'codec_type':'audio','codec_name':'apple_apac'}],
            [{'index':1,'codec_type':'audio','codec_name':'apple_apac','disposition':{'default':1}},
             {'index':2,'codec_type':'audio','codec_name':'aac','disposition':{'default':0}}],
        ):
            with self.subTest(streams=streams):self.assertEqual(audio_maps({'streams':streams}),['0:a?'])
