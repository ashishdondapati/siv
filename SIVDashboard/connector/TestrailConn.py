from connector.testrail import APIClient

import json
import requests
import argparse
import configparser
import logging

config = configparser.ConfigParser()
config.read(r'C:\ETL\SIVDashboard\connector\testrail.ini')


class testrail_authentication:
    """
    Prepare an authentication to use API functions
    In case of APIs to be used for ETL, ID/PW is not needed
    """
    client = APIClient(config.get('Testrail','base_url'))
    def __init__(self):
        self.client.user = config.get('Testrail','client_id')
        self.client.password = config.get('Testrail','client_secret')

    def signIn(self):
        try:
            case = self.client.send_get('get_user_by_email&email=%s' % (self.client.user))
        except Exception as e:
            e = str(e)
            case = {'id': 'none', 'name': 'none', 'email': 'none', 'is_active': 'none', 'errorMsg': e, 'errorCode': return_code(e)}
        finally:
            return case

class testrail_connector:
    """
    Preperation object for getting data using APIs
    """
    client = APIClient(config.get('Testrail','base_url'))
    def __init__(self):
        """
        From ini file, Get the Credential Information for Testrail
        """
        self.client.user = config.get('Testrail','client_id')
        self.client.password = config.get('Testrail','client_secret')

    def get_projects(self):
        """
        Get all projects information which user has permissions to access availble.
        """
        try:
            contents = self.client.send_get('get_projects')
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case

    def get_milestones(self, project_id):
        """
        Get all milestone list by project id
        Argument : project id
        """
        try:
            contents = self.client.send_get('get_milestones/%d' % (project_id))
            case = {'contents': contents, 'errorMsg': '', 'errorCode': '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case

    def get_milestone(self, milestone_id):
        """
        Get all milestone information by milestone id
        Argument : milestone id
        """
        try:
            contents = self.client.send_get('get_milestone/%d' % (milestone_id))
            case = {'contents': contents, 'errorMsg': '', 'errorCode': '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': {"milestones": []}, 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case

    def get_plans(self, project_id, submilestone_id):
        """
        Get all plan list by submilestone id
        Argument : project id, submilestone id
        """
        try:
            contents = self.client.send_get('get_plans/%d&milestone_id=%d' % (project_id, submilestone_id))
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case

    def get_plan(self, planID):
        """
        Get all plan information by plan id
        Argument : plan id
        """
        try:
            contents = self.client.send_get('get_plan/%d' % (planID))
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': {'entries': []}, 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case

    def get_tests(self, runID):
        """
        Get all test list by run id
        Argument : run id
        """
        try:
            contents = self.client.send_get('get_tests/%d' % (runID))
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            print("exception at %s" %runID)
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case


    def get_sections(self, project_id, suite_id):
        """
        Get all section list by suite id
        Argument : project id, suite id
        """
        try:
            print('suite:', suite_id)
            contents = self.client.send_get('get_sections/%d&suite_id=%d' % (project_id, suite_id))
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case


    def get_cases(self, project_id, suite_id):
        """
        Get all case list by suite id
        Argument : project id, suite id
        """
        try:
            contents = self.client.send_get('get_cases/%d&suite_id=%d' % (project_id, suite_id))
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case

    def get_results(self, run_id):
        """
        Get all defects list by run id
        Argument : run id
        """
        try:
            contents = self.client.send_get('get_results_for_run/%d' % (run_id))
            case = {'contents' : contents, 'errorMsg' : '', 'errorCode' : '200'}
        except Exception as e:
            logging.debug(e)
            e = str(e)
            case = {'contents': [], 'errorMsg' : e, 'errorCode': self.client.status_code}
        finally:
            return case
