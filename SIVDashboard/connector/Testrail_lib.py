#-*-coding:utf-8-*-
import unittest
import os, sys
import pandas as pd
from connector.TestrailConn import *
from connector.SQLManager import *

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
arr_projectID_K = [119,164,182,225]
arr_projectID_T = [180]

rt = testrail_connector()


class testrail_modeling:
    def extract(self, type_area):
        if type_area == 'K':
            project_list = arr_projectID_K
        elif type_area == 'T':
            project_list = arr_projectID_T
        #Init List for each APIs
        data_set_milestones = []
        data_set_submilestones = []
        data_set_plans = []
        data_set_runs = []
        data_set_tests = []
        data_set_sections = []
        data_set_cases = []
        data_set_results = []
        suiteIds = {}


        ########################################SIV_K########################################
        if type_area == 'K':
            #Get Milestone Data into the dataset_milestones as a dataframe  ##Start
            try:
                for project_id in project_list:
                    rs = rt.get_milestones(project_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_milestones(project_id)["contents"]
                    for index in range(len(rs)):
                        data_set_milestones.append([
                            rs[index]['id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['start_on'],
                            rs[index]['started_on'],
                            rs[index]['is_started'],
                            rs[index]['due_on'],
                            rs[index]['is_completed'],
                            rs[index]['completed_on'],
                            rs[index]['project_id'],
                            rs[index]['parent_id'],
                            rs[index]['url']
                        ])
                dataset_milestones = pd.DataFrame(data_set_milestones)
                dataset_milestones.columns = [
                    'Milestone.id',
                    'Milestone.name',
                    'Milestone.description',
                    'Milestone.start_on',
                    'Milestone.started_on',
                    'Milestone.is_started',
                    'Milestone.due_on',
                    'Milestone.is_completed',
                    'Milestone.completed_on',
                    'Milestone.project_id',
                    'Milestone.parent_id',
                    'Milestone.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract milestones data.\n :: %s" % e.__str__()
                print(error_msg)
            #Milestone End

            #Get SubMilestone Data into the dataset_submilestones as a dataframe  ##Start
            try:
                for milestone_id in dataset_milestones['Milestone.id']:
                    rs = rt.get_milestone(milestone_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"]["milestones"] if rs['errorCode'] == '200' else rt.get_milestone(milestone_id)["contents"]["milestones"]
                    for index in range(len(rs)):
                        data_set_submilestones.append([
                            rs[index]['id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['start_on'],
                            rs[index]['started_on'],
                            rs[index]['is_started'],
                            rs[index]['due_on'],
                            rs[index]['is_completed'],
                            rs[index]['completed_on'],
                            rs[index]['project_id'],
                            rs[index]['parent_id'],
                            rs[index]['url']
                        ])
                dataset_submilestones = pd.DataFrame(data_set_submilestones)
                dataset_submilestones.columns = [
                    'SubMilestone.id',
                    'SubMilestone.name',
                    'SubMilestone.description',
                    'SubMilestone.start_on',
                    'SubMilestone.started_on',
                    'SubMilestone.is_started',
                    'SubMilestone.due_on',
                    'SubMilestone.is_completed',
                    'SubMilestone.completed_on',
                    'SubMilestone.project_id',
                    'SubMilestone.parent_id',
                    'SubMilestone.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract submilestones data.\n :: %s" % e.__str__()
                print(error_msg)
            #Submilestone End
            
            #Get Plans into the dataset_plans as a dataframe  ##Start
            try:
                for submilestone_id, project_id in zip(dataset_submilestones['SubMilestone.id'], dataset_submilestones['SubMilestone.project_id'] ):
                    rs = rt.get_plans(project_id, submilestone_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_plans(project_id, submilestone_id)["contents"]
                    for index in range(len(rs)):
                        data_set_plans.append([
                            rs[index]['id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['milestone_id'],
                            rs[index]['assignedto_id'],
                            rs[index]['is_completed'],
                            rs[index]['completed_on'],
                            rs[index]['passed_count'],
                            rs[index]['blocked_count'],
                            rs[index]['untested_count'],
                            rs[index]['retest_count'],
                            rs[index]['failed_count'],
                            rs[index]['custom_status1_count'],
                            rs[index]['custom_status2_count'],
                            rs[index]['custom_status3_count'],
                            rs[index]['custom_status4_count'],
                            rs[index]['custom_status5_count'],
                            rs[index]['custom_status6_count'],
                            rs[index]['custom_status7_count'],
                            rs[index]['project_id'],
                            rs[index]['created_on'],
                            rs[index]['created_by'],
                            rs[index]['url']
                        ])
                dataset_plans = pd.DataFrame(data_set_plans)
                dataset_plans.columns = [
                    'Plan.id',
                    'Plan.name',
                    'Plan.description',
                    'Plan.milestone_id',
                    'Plan.assignedto_id',
                    'Plan.is_completed',
                    'Plan.completed_on',
                    'Plan.passed_count',
                    'Plan.blocked_count',
                    'Plan.untested_count',
                    'Plan.retest_count',
                    'Plan.failed_count',
                    'Plan.custom_status1_count',
                    'Plan.custom_status2_count',
                    'Plan.custom_status3_count',
                    'Plan.custom_status4_count',
                    'Plan.custom_status5_count',
                    'Plan.custom_status6_count',
                    'Plan.custom_status7_count',
                    'Plan.project_id',
                    'Plan.created_on',
                    'Plan.created_by',
                    'Plan.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract plans data.\n :: %s" % e.__str__()
                print(error_msg)
            #Plans End
                
            #Get Runs into the dataset_runs as a dataframe  ##Start
            try:
                for plan_id in dataset_plans['Plan.id']:
                    rs = rt.get_plan(plan_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200 
                    rs = rs["contents"]["entries"] if rs['errorCode'] == '200' else rt.get_plan(plan_id)["contents"]["entries"]
                    for index in range(len(rs)):
                        for run_index in range(len(rs[index]['runs'])):
                            data_set_runs.append([
                            rs[index]['runs'][run_index]['id'],
                            rs[index]['runs'][run_index]['suite_id'],
                            rs[index]['runs'][run_index]['name'],
                            rs[index]['runs'][run_index]['description'],
                            rs[index]['runs'][run_index]['milestone_id'],
                            rs[index]['runs'][run_index]['assignedto_id'],
                            rs[index]['runs'][run_index]['include_all'],
                            rs[index]['runs'][run_index]['is_completed'],
                            rs[index]['runs'][run_index]['completed_on'],
                            rs[index]['runs'][run_index]['passed_count'],
                            rs[index]['runs'][run_index]['blocked_count'],
                            rs[index]['runs'][run_index]['untested_count'],
                            rs[index]['runs'][run_index]['retest_count'],
                            rs[index]['runs'][run_index]['failed_count'],
                            rs[index]['runs'][run_index]['custom_status1_count'],
                            rs[index]['runs'][run_index]['custom_status2_count'],
                            rs[index]['runs'][run_index]['custom_status3_count'],
                            rs[index]['runs'][run_index]['custom_status4_count'],
                            rs[index]['runs'][run_index]['custom_status5_count'],
                            rs[index]['runs'][run_index]['custom_status6_count'],
                            rs[index]['runs'][run_index]['custom_status7_count'],
                            rs[index]['runs'][run_index]['project_id'],
                            rs[index]['runs'][run_index]['plan_id'],
                            rs[index]['runs'][run_index]['entry_index'],
                            rs[index]['runs'][run_index]['entry_id'],
                            rs[index]['runs'][run_index]['config'],
                            rs[index]['runs'][run_index]['config_ids'],
                            rs[index]['runs'][run_index]['created_on'],
                            rs[index]['runs'][run_index]['refs'],
                            rs[index]['runs'][run_index]['created_by'],
                            rs[index]['runs'][run_index]['url']
                            ])
                dataset_runs = pd.DataFrame(data_set_runs)
                dataset_runs.columns = [
                    'runs.id',
                    'runs.suite_id',
                    'runs.name',
                    'runs.description',
                    'runs.milestone_id',
                    'runs.assignedto_id',
                    'runs.include_all',
                    'runs.is_completed',
                    'runs.completed_on',
                    'runs.passed_count',
                    'runs.blocked_count',
                    'runs.untested_count',
                    'runs.retest_count',
                    'runs.failed_count',
                    'runs.custom_status1_count',
                    'runs.custom_status2_count',
                    'runs.custom_status3_count',
                    'runs.custom_status4_count',
                    'runs.custom_status5_count',
                    'runs.custom_status6_count',
                    'runs.custom_status7_count',
                    'runs.project_id',
                    'runs.plan_id',
                    'runs.entry_index',
                    'runs.entry_id',
                    'runs.config',
                    'runs.config_ids',
                    'runs.created_on',
                    'runs.refs',
                    'runs.created_by',
                    'runs.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract runs data.\n :: %s" % e.__str__()
                print(error_msg)
            #Runs End
            
            #Get Tests into the dataset_tests as a dataframe  ##Start
            try:
                for run_id in dataset_runs['runs.id']:
                    rs = rt.get_tests(run_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_tests(run_id)["contents"]
                    for index in range(len(rs)):
                        data_set_tests.append([
                            rs[index]['id'],
                            rs[index]['case_id'],
                            rs[index]['status_id'],
                            rs[index]['assignedto_id'],
                            rs[index]['run_id'],
                            rs[index]['title'],
                            rs[index]['template_id'],
                            rs[index]['type_id'],
                            rs[index]['priority_id'],
                            rs[index]['estimate'],
                            rs[index]['estimate_forecast'],
                            rs[index]['refs'],
                            rs[index]['milestone_id'],
                            rs[index]['custom_automationstatus'],
                            rs[index]['custom_hpsm_component_area'],
                            rs[index]['custom_coverage'],
                            rs[index]['custom_weight'],
                            rs[index]['custom_location'],
                            rs[index]['custom_targetproject'],
                            rs[index]['custom_tctype'],
                            rs[index]['custom_custom_a'],
                            rs[index]['custom_custom_b'],
                            rs[index]['custom_tags'],
                            rs[index]['custom_legacytoolid'],
                            rs[index]['custom_mission'],
                            rs[index]['custom_goals'],
                            rs[index]['custom_preconds'],
                            rs[index]['custom_procedure_attachments'],
                            rs[index]['custom_output_attachments'],
                            rs[index]['custom_steps_separated']
                        ])
                dataset_tests = pd.DataFrame(data_set_tests)
                dataset_tests.columns = [
                    'tests.id',
                    'tests.case_id',
                    'tests.status_id',
                    'tests.assignedto_id',
                    'tests.run_id',
                    'tests.title',
                    'tests.template_id',
                    'tests.type_id',
                    'tests.priority_id',
                    'tests.estimate',
                    'tests.estimate_forecast',
                    'tests.refs',
                    'tests.milestone_id',
                    'tests.custom_automationstatus',
                    'tests.custom_hpsm_component_area',
                    'tests.custom_coverage',
                    'tests.custom_weight',
                    'tests.custom_location',
                    'tests.custom_targetproject',
                    'tests.custom_tctype',
                    'tests.custom_custom_a',
                    'tests.custom_custom_b',
                    'tests.custom_tags',
                    'tests.custom_legacytoolid',
                    'tests.custom_mission',
                    'tests.custom_goals',
                    'tests.custom_preconds',
                    'tests.custom_procedure_attachments',
                    'tests.custom_output_attachments',
                    'tests.custom_steps_separated']
            except Exception as e:
                error_msg = "[Error] Fail to extract tests data.\n :: %s" % e.__str__()
                print(error_msg)
            #Tests End
            return dataset_milestones, dataset_submilestones, dataset_plans, dataset_runs, dataset_tests
        ########################################SIV_K########################################
        
        ########################################SIV_T########################################
        if type_area == 'T':
            #Get Milestone Data into the dataset_milestones as a dataframe  ##Start
            try:
                for project_id in project_list:
                    rs = rt.get_milestones(project_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_milestones(project_id)["contents"]
                    for index in range(len(rs)):
                        data_set_milestones.append([
                            rs[index]['id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['start_on'],
                            rs[index]['started_on'],
                            rs[index]['is_started'],
                            rs[index]['due_on'],
                            rs[index]['is_completed'],
                            rs[index]['completed_on'],
                            rs[index]['project_id'],
                            rs[index]['parent_id'],
                            rs[index]['url']
                        ])
                dataset_milestones = pd.DataFrame(data_set_milestones)
                dataset_milestones.columns = [
                    'Milestone.id',
                    'Milestone.name',
                    'Milestone.description',
                    'Milestone.start_on',
                    'Milestone.started_on',
                    'Milestone.is_started',
                    'Milestone.due_on',
                    'Milestone.is_completed',
                    'Milestone.completed_on',
                    'Milestone.project_id',
                    'Milestone.parent_id',
                    'Milestone.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract milestones data.\n :: %s" % e.__str__()
                print(error_msg)
            #Milestone End

            #Get SubMilestone Data into the dataset_submilestones as a dataframe  ##Start
            try:
                for milestone_id in dataset_milestones['Milestone.id']:
                    rs = rt.get_milestone(milestone_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"]["milestones"] if rs['errorCode'] == '200' else rt.get_milestone(milestone_id)["contents"]["milestones"]
                    for index in range(len(rs)):
                        data_set_submilestones.append([
                            rs[index]['id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['start_on'],
                            rs[index]['started_on'],
                            rs[index]['is_started'],
                            rs[index]['due_on'],
                            rs[index]['is_completed'],
                            rs[index]['completed_on'],
                            rs[index]['project_id'],
                            rs[index]['parent_id'],
                            rs[index]['url']
                        ])
                dataset_submilestones = pd.DataFrame(data_set_submilestones)
                dataset_submilestones.columns = [
                    'SubMilestone.id',
                    'SubMilestone.name',
                    'SubMilestone.description',
                    'SubMilestone.start_on',
                    'SubMilestone.started_on',
                    'SubMilestone.is_started',
                    'SubMilestone.due_on',
                    'SubMilestone.is_completed',
                    'SubMilestone.completed_on',
                    'SubMilestone.project_id',
                    'SubMilestone.parent_id',
                    'SubMilestone.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract submilestones data.\n :: %s" % e.__str__()
                print(error_msg)
            #Submilestone End
            
            #Get Plans into the dataset_plans as a dataframe  ##Start
            try:
                for submilestone_id, project_id in zip(dataset_submilestones['SubMilestone.id'], dataset_submilestones['SubMilestone.project_id'] ):
                    rs = rt.get_plans(project_id, submilestone_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_plans(project_id, submilestone_id)["contents"] 
                    for index in range(len(rs)):
                        data_set_plans.append([
                            rs[index]['id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['milestone_id'],
                            rs[index]['assignedto_id'],
                            rs[index]['is_completed'],
                            rs[index]['completed_on'],
                            rs[index]['passed_count'],
                            rs[index]['blocked_count'],
                            rs[index]['untested_count'],
                            rs[index]['retest_count'],
                            rs[index]['failed_count'],
                            rs[index]['custom_status1_count'],
                            rs[index]['custom_status2_count'],
                            rs[index]['custom_status3_count'],
                            rs[index]['custom_status4_count'],
                            rs[index]['custom_status5_count'],
                            rs[index]['custom_status6_count'],
                            rs[index]['custom_status7_count'],
                            rs[index]['project_id'],
                            rs[index]['created_on'],
                            rs[index]['created_by'],
                            rs[index]['url']
                        ])
                dataset_plans = pd.DataFrame(data_set_plans)
                dataset_plans.columns = [
                    'Plan.id',
                    'Plan.name',
                    'Plan.description',
                    'Plan.milestone_id',
                    'Plan.assignedto_id',
                    'Plan.is_completed',
                    'Plan.completed_on',
                    'Plan.passed_count',
                    'Plan.blocked_count',
                    'Plan.untested_count',
                    'Plan.retest_count',
                    'Plan.failed_count',
                    'Plan.custom_status1_count',
                    'Plan.custom_status2_count',
                    'Plan.custom_status3_count',
                    'Plan.custom_status4_count',
                    'Plan.custom_status5_count',
                    'Plan.custom_status6_count',
                    'Plan.custom_status7_count',
                    'Plan.project_id',
                    'Plan.created_on',
                    'Plan.created_by',
                    'Plan.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract plans data.\n :: %s" % e.__str__()
                print(error_msg)
            #Plans End
                
            #Get Runs into the dataset_runs as a dataframe  ##Start
            try:
                for plan_id in dataset_plans['Plan.id']:
                    rs = rt.get_plan(plan_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"]["entries"] if rs['errorCode'] == '200' else rt.get_plan(plan_id)["contents"]["entries"]
                    for index in range(len(rs)):
                        for run_index in range(len(rs[index]['runs'])):
                            sId = rs[index]['runs'][run_index]['suite_id']
                            if sId not in suiteIds.values():
                                suiteIds["'" + rs[index]['runs'][run_index]['id'].__str__() + "'"] = sId
                            data_set_runs.append([
                            rs[index]['runs'][run_index]['id'],
                            rs[index]['runs'][run_index]['suite_id'],
                            rs[index]['runs'][run_index]['name'],
                            rs[index]['runs'][run_index]['description'],
                            rs[index]['runs'][run_index]['milestone_id'],
                            rs[index]['runs'][run_index]['assignedto_id'],
                            rs[index]['runs'][run_index]['include_all'],
                            rs[index]['runs'][run_index]['is_completed'],
                            rs[index]['runs'][run_index]['completed_on'],
                            rs[index]['runs'][run_index]['passed_count'],
                            rs[index]['runs'][run_index]['blocked_count'],
                            rs[index]['runs'][run_index]['untested_count'],
                            rs[index]['runs'][run_index]['retest_count'],
                            rs[index]['runs'][run_index]['failed_count'],
                            rs[index]['runs'][run_index]['custom_status1_count'],
                            rs[index]['runs'][run_index]['custom_status2_count'],
                            rs[index]['runs'][run_index]['custom_status3_count'],
                            rs[index]['runs'][run_index]['custom_status4_count'],
                            rs[index]['runs'][run_index]['custom_status5_count'],
                            rs[index]['runs'][run_index]['custom_status6_count'],
                            rs[index]['runs'][run_index]['custom_status7_count'],
                            rs[index]['runs'][run_index]['project_id'],
                            rs[index]['runs'][run_index]['plan_id'],
                            rs[index]['runs'][run_index]['entry_index'],
                            rs[index]['runs'][run_index]['entry_id'],
                            rs[index]['runs'][run_index]['config'],
                            rs[index]['runs'][run_index]['config_ids'],
                            rs[index]['runs'][run_index]['created_on'],
                            rs[index]['runs'][run_index]['refs'],
                            rs[index]['runs'][run_index]['created_by'],
                            rs[index]['runs'][run_index]['url']
                            ])
                dataset_runs = pd.DataFrame(data_set_runs)
                dataset_runs.columns = [
                    'runs.id',
                    'runs.suite_id',
                    'runs.name',
                    'runs.description',
                    'runs.milestone_id',
                    'runs.assignedto_id',
                    'runs.include_all',
                    'runs.is_completed',
                    'runs.completed_on',
                    'runs.passed_count',
                    'runs.blocked_count',
                    'runs.untested_count',
                    'runs.retest_count',
                    'runs.failed_count',
                    'runs.custom_status1_count',
                    'runs.custom_status2_count',
                    'runs.custom_status3_count',
                    'runs.custom_status4_count',
                    'runs.custom_status5_count',
                    'runs.custom_status6_count',
                    'runs.custom_status7_count',
                    'runs.project_id',
                    'runs.plan_id',
                    'runs.entry_index',
                    'runs.entry_id',
                    'runs.config',
                    'runs.config_ids',
                    'runs.created_on',
                    'runs.refs',
                    'runs.created_by',
                    'runs.url']
            except Exception as e:
                error_msg = "[Error] Fail to extract runs data.\n :: %s" % e.__str__()
                print(error_msg)
            #Runs End

            try:
                # Get Cases  Sections  into the dataset_cases and data_sections as a dataframe  ##Start
                for rId, sId in suiteIds.items():
                    rs = rt.get_cases(project_id, sId)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_cases(project_id, sId)['contents']
                    for index in range(len(rs)):
                        data_set_cases.append([
                            rs[index]["id"],
                            rs[index]["title"],
                            rs[index]["section_id"],
                            rs[index]["template_id"],
                            rs[index]["type_id"],
                            rs[index]["priority_id"],
                            rs[index]["milestone_id"],
                            rs[index]["refs"],
                            rs[index]["created_by"],
                            rs[index]["created_on"],
                            rs[index]["updated_by"],
                            rs[index]["updated_on"],
                            rs[index]["estimate"],
                            rs[index]["estimate_forecast"],
                            rs[index]["suite_id"],
                            rs[index]["display_order"]])
                    dataset_cases = pd.DataFrame(data_set_cases)
                    dataset_cases.columns = [

                        'cases.id',
                        'cases.title',
                        'cases.section_id',
                        'cases.template_id',
                        'cases.type_id',
                        'cases.priority_id',
                        'cases.milestone_id',
                        'cases.refs',
                        'cases.created_by',
                        'cases.created_on',
                        'cases.updated_by',
                        'cases.updated_on',
                        'cases.estimate',
                        'cases.estimate_forecast',
                        'cases.suite_id',
                        'cases.display_order']
                    # Cases Ends

                    # Get Sections into the dataset_cases and data_sections as a dataframe  ##Start
                    rs = rt.get_sections(project_id, sId)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_sections(project_id, sId)["contents"]
                    for index in range(len(rs)):
                        data_set_sections.append([
                            rs[index]['id'],
                            int(rId.replace("'", "")),
                            rs[index]['suite_id'],
                            rs[index]['name'],
                            rs[index]['description'],
                            rs[index]['parent_id'],
                            rs[index]['display_order'],
                            rs[index]['depth']])
                    dataset_sections = pd.DataFrame(data_set_sections)
                    dataset_sections.columns = [
                        'sections.id',
                        'sections.run_id',
                        'sections.suite_id',
                        'sections.name',
                        'sections.description',
                        'sections.parent_id',
                        'sections.display_order',
                        'sections.depth']
                    #Sections Ends

                    # Get Results into the dataset_cases and data_sections as a dataframe  ##Start
                    rs_defect = rt.get_results(int(rId.replace("'", "")))
                    rs_defect = rs_defect["contents"]
                    for i in range(len(rs_defect)):
                        # if rs_defect[i]['defects'] is not None:
                        data_set_results.append([
                            rs_defect[i]['id'],
                            rs_defect[i]['test_id'],
                            int(rId.replace("'", "")),
                            rs_defect[i]['status_id'],
                            rs_defect[i]['defects']])
                    dataset_results = pd.DataFrame(data_set_results)
                    dataset_results.columns = [
                        'results.id',
                        'results.test_id',
                        'results.run_id',
                        'results.status_id',
                        'results.defects'
                    ]
                    #Results End

            except Exception as e:
                error_msg = "[Error] Fail to extract results data.\n :: %s" % e.__str__()
                print(error_msg)
            # Cases, Sections and Results End

            # Get Tests into the dataset_tests as a dataframe  ##Start
            try:
                for run_id in dataset_runs['runs.id']:
                    rs = rt.get_tests(run_id)
                    # TestRail API sometime returns status 400, so  retry API call once if status code is not 200
                    rs = rs["contents"] if rs['errorCode'] == '200' else rt.get_tests(run_id)["contents"]
                    for index in range(len(rs)):
                        data_set_tests.append([
                            rs[index]['id'],
                            rs[index]['case_id'],
                            rs[index]['status_id'],
                            rs[index]['assignedto_id'],
                            rs[index]['run_id'],
                            rs[index]['title'],
                            rs[index]['template_id'],
                            rs[index]['type_id'],
                            rs[index]['priority_id'],
                            rs[index]['estimate'],
                            rs[index]['estimate_forecast'],
                            rs[index]['refs'],
                            rs[index]['milestone_id'],
                            rs[index]['custom_testcasestatus'],
                            rs[index]['custom_coverage'],
                            rs[index]['custom_automation_percentage'],
                            rs[index]['custom_auto_config'],
                            rs[index]['custom_test_level'],
                            rs[index]['custom_workflow'],
                            rs[index]['custom_tags'],
                            rs[index]['custom_external_tool_id'],
                            rs[index]['custom_legacytoolid'],
                            rs[index]['custom_tr_id'],
                            rs[index]['custom_description'],
                            rs[index]['custom_mission'],
                            rs[index]['custom_goals'],
                            rs[index]['custom_comments'],
                            rs[index]['custom_preconds'],
                            rs[index]['custom_required_equipment'],
                            rs[index]['custom_steps_separated'],
                            rs[index]['custom_steps'],
                            rs[index]['custom_expected']
                        ])
                dataset_tests = pd.DataFrame(data_set_tests)
                dataset_tests.columns = [
                    'tests.id',
                    'tests.case_id',
                    'tests.status_id',
                    'tests.assignedto_id',
                    'tests.run_id',
                    'tests.title',
                    'tests.template_id',
                    'tests.type_id',
                    'tests.priority_id',
                    'tests.estimate',
                    'tests.estimate_forecast',
                    'tests.refs',
                    'tests.milestone_id',
                    'tests.custom_testcasestatus',
                    'tests.custom_coverage',
                    'tests.custom_automation_percentage',
                    'tests.custom_auto_config',
                    'tests.custom_test_level',
                    'tests.custom_workflow',
                    'tests.custom_tags',
                    'tests.custom_external_tool_id',
                    'tests.custom_legacytoolid',
                    'tests.custom_tr_id',
                    'tests.custom_description',
                    'tests.custom_mission',
                    'tests.custom_goals',
                    'tests.custom_comments',
                    'tests.custom_preconds',
                    'tests.custom_required_equipment',
                    'tests.custom_steps_separated',
                    'tests.custom_steps',
                    'tests.custom_expected']
            except Exception as e:
                error_msg = "[Error] Fail to extract tests data.\n :: %s" % e.__str__()
                print(error_msg)
            #Tests End
            return dataset_milestones, dataset_submilestones, dataset_plans, dataset_runs, dataset_tests, dataset_sections, dataset_cases, dataset_results
        ########################################SIV_T########################################
    def submitSQL(self, df, tableName):
        """Modeling data using dynamic query.

        Args:

        Returns:

        """
        db_ins = SQLManager()
        exist_statement = 'SELECT COUNT(*) FROM %s' % tableName
        rows = db_ins.runQuery(exist_statement)
        if rows == 0:
            create_statement = 'CREATE TABLE %s (dummy int NOT NULL)' % tableName
            try :
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