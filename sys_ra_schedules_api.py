import copy
import os
import pytest

from allpairspy import AllPairs
from pathlib import Path

from ha_pytest.util import request_factory as req

from wmas_utils import project_helper as p_helper
from ha_pytest.util.anypoint import helper as ap_helper


def get_project_dir():
    return str(Path(os.path.dirname(os.path.realpath(__file__))).parent)


# Params as a fixture - do not use directly see _get_working_params which provides deepcopy and sets load_test
# fixtures don't take parameters so load_test is set in get working params
@pytest.fixture(scope='module')
def suite_params():
    suite_params = p_helper.setup_params(request_type='GET', project_dir=get_project_dir())

    suite_params.headers = {'x-ha-business-context': 'Schedules', 'Content-Type': 'application/json',
                            'x-root-correlation-id': 'test-sys-ra-schedules'}

    # increase the timeout to 90 seconds.  One of the schedule calls took 41 seconds so let's be generous to avoid
    # bamboo failures
    suite_params.sla = 90000

    return suite_params

def functional_all_stations_positive( station, full_update, params_in, load_test=None):
    params = copy.deepcopy(params_in)
    if load_test is not None:
        params.load_test = True
    params.expected_status = 200
    params.expected_response = ['sentDate']

    params.query_param = QUERY_PARAMS

    params.query_param.update({'fullUpdate': full_update})
    params.query_param.update({'locationName': station})

    result = req.verify_request(params)

    assert result.test_passed, result.message
    

# schedulesUpdates?startDate=2020-04-07T00:00:00&endDate=2020-05-09T23:59:59&locationName=PDX&fullUpdate=false
QUERY_PARAMS = {'startDate': '2021-01-01T00:00:00', 'endDate': '2021-02-01T23:59:59',
                'locationName': 'PDX', 'fullUpdate': 'false'}


@pytest.mark.parametrize(['sent_date', 'status', 'expected'],
                         [('2026-10-30T21:40', 200, ['Confirm']), ('2026-10-30T21:4', 500, ['error'])])
def test_post_sent_date(sent_date, status, expected, suite_params):
    params = copy.deepcopy(suite_params)

    # Change action to POST from default GET
    params.request_type = 'POST'
    params.query_param = None
    params.expected_status = status
    params.expected_response = expected

    # Set the confirmation endpoint
    params.request_str += '/' + sent_date + '/confirm'

    # The api has slightly different endpoint names on the two HTTP actions
    params.request_str = params.request_str.replace('schedulesUpdates', 'scheduleUpdates')

    result = req.verify_request(params)

    assert result.test_passed, result.message


@pytest.mark.parametrize('key', QUERY_PARAMS.keys())
def test_get_schedules_missing_query_param_pop_negative(key, suite_params):
    params = copy.deepcopy(suite_params)
    argument_missing = QUERY_PARAMS.copy()
    argument_missing.pop(key)

    params.query_param = argument_missing
    params.expected_status = 400
    params.expected_response = ['error']

    result = req.verify_request(params)

    assert result.test_passed, result.message


ALL_STATIONS = ['OGG', 'LIH', 'LAS', 'PHX', 'LAX', 'SAN', 'SFO', 'SJC', 'SEA', 'OAK', 'PDX', 'SMF', 'ASHI']
ALL_STATIONS_OPTIONS = [STATIONS, ['false', 'true']]


@pytest.mark.parametrize(['station', 'full_update'], [values for values in AllPairs(ALL_STATIONS_OPTIONS)])
def test_get_all_stations_positive(station, full_update, suite_params):
    # Create a test that iterates every station and does it for both fullUpdate true and false
    params = copy.deepcopy(suite_params)

    params.expected_status = 200
    params.expected_response = ['sentDate']

    params.query_param = QUERY_PARAMS

    params.query_param.update({'fullUpdate': full_update})
    params.query_param.update({'locationName': station})

    result = req.verify_request(params)

    assert result.test_passed, result.message


@pytest.mark.parametrize('key', QUERY_PARAMS.keys())
def test_get_schedules_missing_query_param_pop_negative_logs(key, suite_params):
    # test case for "400: Bad Request" in logs
    params = copy.deepcopy(suite_params)

    argument_missing = QUERY_PARAMS.copy()
    argument_missing.pop(key)

    params.query_param = argument_missing
    params.expected_status = 400
    params.expected_response = ['error']

    result = req.verify_request(params)
    message_to_verify = "Bad request"
    logs = ap_helper.wait_for_message_in_logs(params.app_name, params.env, message_to_verify)
    log_cycle = ap_helper.get_log_cycle(logs,
                                        correlation_id='test-sys-ra-schedules',
                                        correlation_id_field='rootCorrelationId')
    assert len(log_cycle) > 0


@pytest.mark.parametrize('key', QUERY_PARAMS.keys())
def test_get_schedules_invalid_param_logs(key, suite_params):
    # invalid test case

    params = copy.deepcopy(suite_params)
    arguments = QUERY_PARAMS.copy()
    arguments['startDate'] = "INVALID"

    params.query_param = arguments
    params.expected_status = 400
    params.expected_response = ['error']

    result = req.verify_request(params)
    message_to_verify = "Bad request"
    logs = ap_helper.wait_for_message_in_logs(params.app_name, params.env, message_to_verify)
    log_cycle = ap_helper.get_log_cycle(logs, correlation_id='test-sys-ra-schedules',
                                        correlation_id_field='rootCorrelationId')
    assert len(log_cycle) > 0


WEST_COAST_STATIONS = ['HNL', 'ITO', 'OGG', 'KOA', 'LAX', 'LAS', 'SFO', 'SJC', 'SEA', 'OAK', 'PDX', 'SMF']
WEST_COAST_OPTIONS = [WEST_COAST_STATIONS, ['true']]


@pytest.mark.parametrize(['station', 'full_update'], [values for values in AllPairs(WEST_COAST_OPTIONS)])
def test_get_shift_status_value(station, full_update, suite_params):
    # create a test that verifies shift status field value be present in enum value list
    params = copy.deepcopy(suite_params)

    params.expected_status = 200
    params.expected_response = ['shiftStatus']

    params.query_param = QUERY_PARAMS

    params.query_param.update({'fullUpdate': full_update})
    params.query_param.update({'locationName': station})

    result = req.verify_request(params)
    assert result.test_passed, result.message



@pytest.mark.parametrize(['station', 'full_update'], [values for values in AllPairs(WEST_COAST_OPTIONS)])
def test_get_data_validation_bank_transaction(station, full_update, suite_params):
    # verify the response data validation -ptoBankTransactions
    params = copy.deepcopy(suite_params)

    params.expected_status = 200
    params.expected_response = [{"$..shiftAllData[*].breakDuration": 30, "$..shiftAllData[*].originalOwnerID": 143,
                                 "$..shiftAllData[*].EmployeeID": 143,
                                 "$..shiftAllData[*].RecordVersion": 9077522918941917184,
                                 "$..shiftAllData[*].AbsentHoursAmount": 0}]
    params.query_param = QUERY_PARAMS

    params.query_param.update({'fullUpdate': full_update})
    params.query_param.update({'locationName': station})

    result = req.verify_request(params)

    assert result.test_passed, result.message


OTHER_STATIONS = ['HNL', 'ITO', 'OGG', 'KOA', 'LAX', 'LAS', 'OAK', 'PDX']
OTHER_STATIONS_OPTIONS = [STATIONS, ['true']]


@pytest.mark.parametrize(['station', 'full_update'], [values for values in AllPairs(OTHER_STATIONS_OPTIONS)])
def test_get_all_mandatory_field_display(station, full_update, suite_params):
    # Verify following mandatory fields should be displayed in response when GET request call with required input fields
    params = copy.deepcopy(suite_params)

    params.expected_status = 200
    params.expected_response = ['sentDate', 'employeeNumber', 'startDate', 'endDate', 'countsTowardOvertime',
                                'wasShiftTraded']

    params.query_param = QUERY_PARAMS

    params.query_param.update({'fullUpdate': full_update})
    params.query_param.update({'locationName': station})
    params.query_param.update({'startDate': '2021-01-21T07:00:00'})
    params.query_param.update({'endDate': '2021-01-21T20:00:00'})

    result = req.verify_request(params)

    assert result.test_passed, result.message

