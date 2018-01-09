import unittest
from unittest import TestCase
from z3gpp import Z3gUtils
import requests
import os
import pandas as pd
import traceback


class TestZ3gUtils(TestCase):

    def base_init(self):
        self.zu = Z3gUtils(session=requests.session(),
                           proxy=None,
                           headers={
                            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                                          'Chrome/47.0.2526.80 Safari/537.36',
                            'Accept': 'text/html',
                            'Accept-Encoding': 'gzip'
                            })
        if os.path.exists('result') is False:
            os.mkdir('result')

    def test_get_groups(self):
        self.base_init()
        try:
            res, groups = self.zu.get_groups(force_reload=True)
        except Exception as e:
            print(e)
            traceback.print_exc()
            res = False

        if res is True and type(groups) is pd.DataFrame:
            groups.to_csv('result/groups.csv')
            self.assertEqual(res, True)
        else:
            self.fail()


if __name__ == '__main__':
    unittest.main()
