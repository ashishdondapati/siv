
"""This module handles the retrieve jira and
    Update the data into database

"""

import pyodbc
import requests
import json
import base64
import math
import datetime
import pandas as pd
import configparser
import os, sys
import logging
import time
from connector.SQLManager import SQLManager

cwd = os.getcwd()
ini_path = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
configFile = ini_path + '\\jira.ini'
config.read(configFile)


class JiraConnectionManager:
    def __init__(self):
        self.pf_stories = pd.DataFrame()
        self.stories =[]
        self.pf_bugs = pd.DataFrame()
        self.pf_bugs = pd.DataFrame()

        self.base_url = config.get('Jiraserver', 'base_url')
        self.client_id = config.get('Auth', 'client_id')
        self.auth_token = config.get('Auth', 'AuthToken')
        auth = str(base64.b64encode(bytes(str(self.client_id) + ':' + str(self.auth_token), 'utf-8')), 'ascii').strip()
        self.headers = {
            'Authorization': 'Basic %s' %auth
        }

    def _get_value_of(self, field):
        try:
            return eval(field)
        except Exception as e:
            return None

    def get_issues(self, querystring, bulk_fetch=False):
        """
        Send Get request to given url
        Args: 
            query string: str
        Returns: 
            issues list: list

        """
        url = self.base_url +  querystring
        filter = config.get('Jiraserver', 'updated_time') if not bulk_fetch else ''
        update_filter = ''
        if filter != '':
            update_filter = ' AND updated >= -{}'.format(filter)
        startAt = 0
        total = 0
        issues = []
        while startAt <= total:
            print("Sending Request to : " + url+ update_filter + '&startAt=%s' %(str(startAt)))
            response = requests.request(
                "GET",
                url = url + update_filter + '&startAt=%s' %(str(startAt)),
                headers=self.headers
            )
            response.raise_for_status()
            res = json.loads(response.content)
            startAt = startAt + res['maxResults']
            total =res['total']
            issues.extend(json.loads(response.text)['issues'])
        return issues

    def get_issues_by_labels(self, issueType : str, bug_labels: list, bulk_fetch=False, **kwargs):
        """
        Get Jira Issues for given labels
        Args: 
            IssueType: str, bug__labels: list
        Returns: 
            issues list: list

        """
        keys = list(kwargs.keys())
        values = list(kwargs.values())
        print('issueType=' + issueType)
        concate = 'AND ' + ''.join(key + '=' + value for key, value in zip(keys, values)) if kwargs else ''
        querystring = 'issueType in(' + issueType + ') AND labels in (%s) %s' %(','.join(i for i in bug_labels), concate)
        response = self.get_issues(querystring, bulk_fetch)
        return response


    def get_latest_from_jira(self, type, story=''):
        """Issue a GET request (read) against the API to get all
        the issues for given project based on the filter argument
        with '' (empty) will get all the issues, otherwise get all
        the data updated during the given period ('1h'- last 1 hour update
        Update/insert the records Sql server db

        Returns:
            none
        """

        # calculate the number of API iteration required to get all the items in the response.
        start = 0
        index = 0
        itr_cnt = 1
        maxResults = 50
        self.pf_bugs = pd.DataFrame()
        pf_epic = pd.DataFrame()
        update_filter = ''
        filter = ''
        # period that we need to get the data from jira
        if type.lower() == 'story':
            filter = config.get('Jiraserver', 'updated_time_story')
        else:
            filter = config.get('Jiraserver', 'updated_time')
        if filter != '':
            update_filter = 'AND updated >= -{}'.format(filter)

        try:
            # make API request based on the total items in the response
            while index < itr_cnt:
                start_time = datetime.datetime.now()
                projectKey = config.get('Jiraserver', 'project_key')
                url =''
                if type.lower() == 'epic':
                    url = config.get('Jiraserver', 'epic_url').format(projectKey, update_filter, start)
                elif type.lower() == 'story':
                    url = config.get('Jiraserver', 'story_url').format(projectKey, update_filter, start)
                elif type.lower() == 'bugs':
                    url = config.get('Jiraserver', 'bugs_url').format(story, update_filter, start)
                else:
                    print('issue type will not supported.')
                    return
                print(story)
                print(url)
                response = requests.request(
                    "GET",
                    url,
                    headers=self.headers
                )
                # Decode Json string to Python
                if response.status_code == 200:
                    json_data = json.loads(response.text)

                    # Display issues

                    total_count = json_data["total"]
                    itr_cnt = math.ceil(total_count / maxResults)
                    start = index * maxResults
                    if itr_cnt >= index:
                        index = (index + 1)
                        if type.lower() == 'epic':
                            epic = self.extract_epic(json_data["issues"])
                            pf_epic = pd.concat([pf_epic, epic])

                        elif type.lower() == 'story':
                            stories, dat_story = self.extract_story(json_data["issues"])
                            self.stories = self.stories + stories
                            self.pf_stories = pd.concat([self.pf_stories, dat_story])

                        elif type.lower() == 'bugs':
                            dat_bugs= self.extract_bugs(json_data["issues"])
                            self.pf_bugs = pd.concat([self.pf_bugs, dat_bugs])

                if type.lower() == 'epic':
                    return pf_epic
                elif type.lower() == 'story':
                    return self.stories, self.pf_stories
                elif type.lower() == 'bugs':
                    return self.pf_bugs

        except Exception as e:
            error_msg = "[Error] Fail to extract jira data.\n :: %s" % e.__str__()
            print(error_msg)


    def extract_initiatives(self, items):
        """
        Extract initiative data and update the dataframe
        Args:
             list of Issues(Initiativr items)
        Returns:
            Initiative Dataframe
        """
        initiatives = []
        global init
        try:
            for init in items:
                if init:
                    # extract data from JIRA
                    key = self._get_value_of('init["key"]')
                    summary = self._get_value_of('init["fields"]["summary"]')
                    risk_type = self._get_value_of('init["fields"]["customfield_17351"]["value"]')
                    body = self._get_value_of('init["fields"]["customfield_27500"]')

                    #Transform Extracted Data
                    body_as_html = self.convert_jira_to_html(body) if body else body

                    initiatives.append([
                        key,
                        summary,
                        body_as_html,
                        risk_type
                    ])

            df_initiative = pd.DataFrame(initiatives)
            df_initiative.columns = [
                'KEY',
                'Program Name',
                'Program Summary',
                'Program Status'
            ]
        except Exception as e:
            print(e)
            print('failed to extract initiatives ')
        return df_initiative

    def extract_epic(self, items):
        """
        Extract epic data and update the dataframe
        Args:
             list of Issues(Epic items)
        Returns:
            Epic Dataframe
        """
        data_set_epic = []
        try:
            global item
            for item in items:
                projName = self._get_value_of("item['fields']['project']['name']")
                Key = item['key']
                issueType = self._get_value_of("item['fields']['issuetype']['name']")
                summary = self._get_value_of("item['fields']['summary']")
                status = self._get_value_of("item['fields']['status']['name']")
                parentKey = self._get_value_of("item['fields']['customfield_11738']")
                epicName = self._get_value_of("item['fields']['customfield_10005']")
                testRail_url = self._get_value_of("item['fields']['customfield_29502']")
                
                data_set_epic.append([
                    projName,
                    Key,
                    issueType,
                    summary,
                    status,
                    parentKey,
                    testRail_url
                ])
            dataset_epic = pd.DataFrame(data_set_epic)
            dataset_epic.columns = [
                'JIRA Project',
                'KEY',
                'Issue Type',
                'Epic Name',
                'Epic Status',
                'Linked Initiative',
                'TestRail URL'
            ]
        except Exception as e:
            error_msg = "[Error] Fail to extract epic data.\n :: %s" % e.__str__()
            print(error_msg)
        return dataset_epic

    def extract_story(self, items):
        """
         Extract stories and bugs data and update the dataframe
         Args:
             list of Issues(Story items)
         Returns:
             story and bugs Dataframe
         """
        data_set_story = []
        stories =[]
        bugs = []
        try:
            for item in items:
                projName = item['fields']['project']['name'] if 'project' in item['fields'] else ''
                storyKey = item['key']
                stories.append(storyKey)
                issueType = item['fields']['issuetype']['name'] if 'issuetype' in item['fields'] else ''
                summary = item['fields']['summary'] if 'summary' in item['fields'] else ''
                storyStatus = item['fields']['status']['name'] if 'status' in item['fields'] else ''
                parentKey = item['fields']['customfield_10006'] if 'customfield_10006' in item['fields'] else ''
                links = item['fields']['issuelinks'] if 'issuelinks' in item['fields'] else ''
                key = ''
                epic_name = item['fields']['customfield_10006'] if 'customfield_10006' in item['fields'] else ''
                story_id = item['id']
                updated = item['fields']['updated'] if 'updated' in item['fields'] else ''
                lbls = ''
                if 'labels' in item['fields'] and item['fields']['labels'] is not None:
                    labels = item['fields']['labels']
                lbls = ','.join(map(str, labels))

                data_set_story.append([
                    projName,
                    storyKey,
                    issueType,
                    summary,
                    storyStatus,
                    parentKey,
                    updated,
                    lbls
                ])
            dataset_story = pd.DataFrame(data_set_story)
            dataset_story.columns = [
                'Project Name',
                'KEY',
                'Issue Type',
                'Story Name',
                'Status Detail',
                'Parent Epic ID',
                'Updated Date',
                'Labels'
            ]

        except Exception as e:
            error_msg = "[Error] Fail to extract tests data.\n :: %s" % e.__str__()
            print(error_msg)
        return stories, dataset_story

    def extract_bugs(self, items: list, parent_keys: list=[]):
        """
        Extract bug data and update the dataframe
        Args:
            bug -  bug data
        Returns:
            bug Dataset
        """
        data_set_issue = []
        global bug, item
        try:
            for bug in items:
                if bug is not None:
                    # Extract Values from response
                    comps = self._get_value_of("bug['fields']['components']")
                    assignee = self._get_value_of("bug['fields']['assignee']['displayName']")
                    lbls = self._get_value_of("bug['fields']['labels']")
                    resloution = self._get_value_of("bug['fields']['customfield_13555']['value']")
                    temp = self._get_value_of("bug['fields']['customfield_10605']['value']")
                    links = self._get_value_of("bug['fields']['issuelinks']")
                    ApplicableProducts1 = self._get_value_of("bug['fields']['customfield_14422']")    #Coulumn Applicable Products1
                    allfixVersions = self._get_value_of("bug['fields']['fixVersions']")    #Coulumn fixVersions
                    resolved = self._get_value_of("bug['fields']['resolutiondate']")  #Fix Versions
                    found_in_fw_ver = self._get_value_of("bug['fields']['customfield_11405']")
                    found_in_sw_ver = self._get_value_of("bug['fields']['customfield_11407']")
                    found_in_hw_ver = self._get_value_of("bug['fields']['customfield_11406']")
                    epic_link = self._get_value_of("bug['fields']['customfield_10006']")
                    investment_type = self._get_value_of("bug['fields']['customfield_14903']['value']")
                    reproducibility = self._get_value_of("bug['fields']['customfield_11408']['value']")
                    parent_link_Key=''
                    parent_link_Name=''

                    # Transform Extracted Data                    
                    issuecomps = ''
                    if comps and len(comps) > 0:
                        for i in range(len(comps)):
                            issuecomps = issuecomps + ',' + comps[i]['name'] if issuecomps else comps[i]['name']
                    lbls = ','.join(map(str, lbls)) if lbls else None
                    linked_keys = []
                    linked_key_names = []
                    if links:
                        for item in links:
                            parent_link_Key = self._get_value_of("item['outwardIssue']['key']")
                            parent_link_Name = self._get_value_of("item['outwardIssue']['fields']['summary']")
                            if parent_link_Key is None:
                                parent_link_Key = self._get_value_of("item['inwardIssue']['key']")
                                parent_link_Name = self._get_value_of("item['inwardIssue']['fields']['summary']")
                            if parent_link_Key in parent_keys:
                                linked_keys.append(parent_link_Key)
                                linked_key_names.append(parent_link_Name)
                    if linked_keys:
                        linked_keys = list(set(linked_keys))
                        linked_key_names = list(set(linked_key_names))
                    else:
                        linked_keys.append(None)
                        linked_key_names.append(None)
                    ApplicableProducts1 = ','.join(map(str, ApplicableProducts1)) if ApplicableProducts1 else None   #Coulumn Applicable Products1
                    fixVersions = ''
                    if allfixVersions:
                        for version in allfixVersions:
                            if 'name' in version:
                                ver = version['name']
                                fixVersions = fixVersions + ', %s' %ver if fixVersions else fixVersions + ver

                    #Append data to list
                    data_set_issue.append([
                        bug['fields']['project']['key'] if 'project' in bug['fields'] else '',
                        bug['fields']['project']['name'] if 'project' in bug['fields'] else '',
                        bug['fields']['issuetype']['name'] if 'issuetype' in bug['fields'] else '',
                        bug['key'],
                        bug['fields']['status']['name'] if 'status' in bug['fields'] else '',
                        resloution,
                        bug['fields']['status']['statusCategory']['name'] if 'statusCategory' in bug['fields'][
                            'status'] else '',
                        temp,
                        bug['fields']['summary'] if 'summary' in bug['fields'] else '',
                        bug['fields']['priority']['name'] if 'priority' in bug['fields'] else '',
                        bug['fields']['creator']['displayName'] if 'creator' in bug['fields'] else '',
                        assignee,
                        bug['fields']['created'] if 'created' in bug['fields'] else '',
                        resolved,
                        issuecomps,
                        bug['fields']['updated'] if 'updated' in bug['fields'] else '',
                        ApplicableProducts1,
                        fixVersions,
                        found_in_fw_ver,
                        found_in_sw_ver,
                        found_in_hw_ver,
                        lbls,
                        linked_keys[0],
                        linked_key_names[0],
                        investment_type,
                        reproducibility
                    ])
                    # Insert new row for the issue which has multiple links
                    if len(linked_keys) > 1:
                        new_row = [item for item in data_set_issue[-1]]
                        for i in range(1, len(linked_keys)):
                            new_row[-4] = linked_keys[i]
                            new_row[-3] = linked_key_names[i] 
                            data_set_issue.append([col for col in new_row])
            dataset_bugs = pd.DataFrame(data_set_issue)
            dataset_bugs.columns = [
                'JIRA Project Key',
                'JIRA Project Name',
                'Issue Type',
                'KEY',
                'Status Detail',    
                'Resolution',
                'Validation Status',
                'Severity',
                'Summary',
                'Priority',
                'Issue Creator',
                'Issue Assignee',
                'Date Created',
                'Date Closed',
                'Components',
                'Updated_On',
                'Appl. Product',
                'FixVersions',
                'Found_In_Fw_Version',
                'Found_In_Sw_Version',
                'Found_In_Hw_Version',
                'labels',
                'Linked Epic/Story',
                'Linked Epic/Story Name',
                'Investment Type',
                'Reproducibility'
            ]
            dataset_bugs['Date Created'] = dataset_bugs['Date Created'].astype('datetime64[ns]')
            dataset_bugs['Date Closed'] = dataset_bugs['Date Closed'].astype('datetime64[ns]')
        except Exception as e:
            error_msg = "[Error] Fail to extract Bugs data.\n :: %s" % e.__str__()
            print(error_msg)
        return dataset_bugs

    def getBugs(self, url):
        """Issue a GET request (read) against the API to get
        the issues for given url,
        Args:
            url - url to the issue
        Returns:
            response as json issue data
        """

        try:

            # Send request and get response
            response = requests.request(
                "GET",
                url,
                headers=self.headers
            )
            if response.status_code == 200:
                return json.loads(response.text)

            else:
                return response.text
        except pyodbc.Error as ex:
            sqlstate = ex.args[1]
            print(sqlstate)

    def submitSQL(self, df, tableName):
        """update the table with given dataframe.
        Args: df to update, table name
        Returns:
        """

        db_ins = SQLManager()
        exist_statement = 'SELECT COUNT(*) FROM %s' % tableName
        rows = db_ins.runQuery(exist_statement)
        if rows == 0:
            create_statement = 'CREATE TABLE %s (dummy int NOT NULL)' % tableName
            try:
                db_ins.runQuery(create_statement)
            except Exception as e:
                error_msg = "[Error] Fail to create table.\n :: %s" % e.__str__()
                print(error_msg)

        duplicate_statement = 'SELECT * INTO temp_tr FROM %s' % tableName
        db_ins.runQuery(duplicate_statement)
        delete_statement = 'DROP TABLE temp_tr'

        try:
            df.to_sql(name=tableName, con=db_ins.engine, if_exists='replace', index=False)
            print('Data loaded to %s' %tableName)
            db_ins.runQuery(delete_statement)
        except Exception as e:
            restore_statement = 'CREATE TABLE %s SELECT * FROM temp_tr' % tableName
            db_ins.runQuery(restore_statement)
            db_ins.runQuery(delete_statement)
            error_msg = "[Error] Fail to submit data to table.\n :: %s" % e.__str__()
            print(error_msg)

    def selectSQL(self, tableName):
        """get all the data from the table query.
        Args: tablename
        Returns: table data as dataframe
        """

        db_ins = SQLManager()
        df = pd.read_sql_table(table_name=tableName, con=db_ins.engine)
        return df

        exist_statement = 'SELECT COUNT(*) FROM %s' % tableName
        rows = db_ins.runQuery(exist_statement)
        if rows == 0:
            create_statement = 'CREATE TABLE %s (dummy int NOT NULL)' % tableName
            try:
                db_ins.runQuery(create_statement)
            except Exception as e:
                error_msg = "[Error] Fail to create table.\n :: %s" % e.__str__()
                print(error_msg)

        duplicate_statement = 'SELECT * INTO temp_tr FROM %s' % tableName
        db_ins.runQuery(duplicate_statement)
        delete_statement = 'DROP TABLE temp_tr'

        try:
            df.to_sql(name=tableName, con=db_ins.engine, if_exists='replace', index=False)
            db_ins.runQuery(delete_statement)
        except Exception as e:
            restore_statement = 'CREATE TABLE %s SELECT * FROM temp_tr' % tableName
            db_ins.runQuery(restore_statement)
            db_ins.runQuery(delete_statement)
            error_msg = "[Error] Fail to submit data to table.\n :: %s" % e.__str__()
            print(error_msg)

    def checkTableExists(self, tableName):
        db_ins = SQLManager()
        return db_ins.checkTableExists(tableName)

    def convert_jira_to_html(self, jira_data):
        """
        convert JIRA data to HTML format, to display last comment on PowerBI properly.
        Need to install pandoc(https://pandoc.org/installing.html) in database server, before calling this.
        Args: str
        Returns: html

        """
        #save jira to file
        temp_jira_path = str(time.time()).split('.')[0] + '_jira_temp.txt'
        temp_html_path = temp_jira_path + '.html'
        with open(temp_jira_path, 'wt', encoding='utf8') as f:
            f.write(jira_data)

        #convert to html
        #html_output = pypandoc.convert_file(temp_jira_path,'jira','html')
        #cmd = 'pandoc -s -c %s --metadata title="%s" -f jira %s -o %s' % ('jira.css', 'converted by jira markdown', temp_jira_path, temp_html_path)
        #cmd = 'pandoc -s -c %s -f jira %s -o %s' % ('jira.css', temp_jira_path, temp_html_path)
        cmd = 'pandoc -s -f jira %s -o %s' % ( temp_jira_path, temp_html_path)
        print(cmd)
        os.system(cmd)

        with open(temp_html_path, 'rt', encoding='utf8') as f:
            html_output = f.read()

        #replace color string
        html_output = html_output.replace('<span color="','<span style="color:')
        #insert table css
        html_output = html_output.replace('  </style>', '\n    table, th, td {  border: 1px solid black;}\n  </style>')
        html_output = html_output.replace('○', '&#9675;')
        #define css
        _css = '''
        <style>
        table {
        border-collapse: collapse;
        }
        table, th, td {
        border: 1px solid black;
        }
        </style>
        '''

        #remove temp file
        os.remove(temp_jira_path)
        os.remove(temp_html_path)
        #html = _css + os.linesep + html_output
        html = html_output

        return html

class JiraConnectionManagerError(Exception):
    pass
