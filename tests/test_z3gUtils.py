import os
import traceback
import unittest
from unittest import TestCase

import pandas as pd
import requests

from z3gpp import Z3gUtils


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
            res, groups = self.zu.get_groups()
        except Exception as e:
            print(e)
            traceback.print_exc()
            res = False

        if res is True and type(groups) is pd.DataFrame:
            groups.to_csv('result/groups.csv')
            self.assertEqual(res, True)
        else:
            self.fail()

    def test_get_meetings(self):
        self.base_init()
        try:
            res, meetings = self.zu.get_meetings('r1', force_reload=False)
        except Exception as e:
            print(e)
            traceback.print_exc()
            res = False

        if res is True and type(meetings) is pd.DataFrame:
            meetings.to_csv('result/meetings_ran1.csv')
            self.assertEqual(res, True)
        else:
            self.fail()

    def test_get_tdoc_list(self):
        self.base_init()
        try:
            res, tdocs = self.zu.get_tdoc_list('r1-90', force_reload=False)
        except Exception as e:
            print(e)
            traceback.print_exc()
            res = False

        if res is True and type(tdocs) is pd.DataFrame:
            tdocs.to_csv('result/tdocs_r1-90.csv')
            self.assertEqual(res, True)
        else:
            self.fail()

    def test_get_tdoc(self):
        self.base_init()
        res = False
        try:
            tdocs = self.zu.get_tdoc('r1-90', force_reload=True)
            res = True
        except Exception as e:
            print(e)
            traceback.print_exc()

        if res is True and type(tdocs) is pd.DataFrame:
            tdocs.to_csv('result/tdocs_r1-90.csv')
            self.assertEqual(res, True)
        else:
            self.fail()


if __name__ == '__main__':
    unittest.TestLoader().loadTestsFromName('user.UserTestCase.test_get_tdoc')
    unittest.main()
