'''
    Connect with Facebook's graph api and fetch Nerdwallet page post data!
'''

import argparse
from datetime import datetime
import json
import requests
import pprint
import os
import sys
import time

from fb_graph_api import GraphAPI
from facebook_business import FacebookAdsApi
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adcreative import AdCreative

import fb_modules
from nw_generic_utils_modules import export_to_json_redshift

API_ATTEMPTS = 5
TIMEOUT = 1*60
END_POINTS = ['posts', 'promotable_posts']

# Social
SOCIAL_ACCT = 'act_10155375447713502'


def valid_date(s):
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        msg = "Not a valid date format('yyyy-mm-dd): '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def parse_post_perf_data(post_id, json_feed):
    parsed_json = {}
    parsed_json["post_id"] = post_id
    
    for metric in json_feed:
        value = metric["values"]

        if metric['name'] == "post_consumptions_by_type":
            parsed_json["link_clicks"] = value[0].get("value").get("link clicks")
            parsed_json["video_clicks"] = value[0].get("value").get("video play")
            parsed_json["other_clicks"] = value[0].get("value").get("other clicks")

        elif metric['name'] == "post_consumptions_by_type_unique":
            parsed_json["link_clicks_unique"] = value[0].get("value").get("link clicks")
            parsed_json["video_clicks_unique"] = value[0].get("value").get("video play")
            parsed_json["other_clicks_unique"] = value[0].get("value").get("other clicks")

        else:
            parsed_json[metric["name"]] = value[0]["value"]

    return parsed_json


def get_post_perf_data(graph_api, post_id):
    """

    :param graph_api:
    :param post_id:
    :return:
    """

    metrics = ','.join(CONFIG['metrics'])
    post_resp = graph_api.request(end_points=[post_id, "insights", metrics], params={"period":"lifetime"}, paginate=None)
    if post_resp:
        raw_data, next_url = post_resp
        # pp.pprint(raw_data)
        # pp.pprint(next_url)
        if raw_data.get("data"):

            parsed_data = parse_post_perf_data(post_id, raw_data.get("data"))
            parsed_data["shares"], parsed_data["comments"]  = get_post_data(graph_api, post_id)
            # pp.pprint(parsed_data)
            return parsed_data
    return False


def get_api_data(graph_api, end_point, params):
    ids = []
    raw_data, next_url = graph_api.request(end_points=[CONFIG["page_id"], end_point], params=params, paginate=None)
    # pp.pprint(raw_data)

    # Add ids to list
    ids.extend(raw_data.get("data"))

    count = 1
    while next_url:
        print "Calling next url {0}".format(count)
        raw_data, next_url = graph_api.request(params=params, paginate=next_url)

        # pp.pprint(raw_data)
        ids.extend(raw_data.get("data"))
        time.sleep(1)
        count += 1
    return ids


def get_post_data(graph_api, post_id):
    """
        Return corresponding post data
    :param post_fields:
    :param graph_api:
    :param post_id:
    :return:
    """

    post_params = {'fields':','.join(["id","shares","status_type","comments.limit(0).summary(true)"]), 'period':'lifetime'}
    raw_data, next_url = graph_api.request(end_points=[post_id], params=post_params, paginate=None)
    # pp.pprint(raw_data)
    shares = 0
    comments = 0

    try:
        shares = raw_data["shares"]["count"]
        comments = raw_data["comments"]["summary"]["total_count"]
    except:
        print "Key not found"

    return shares, comments


def get_creative_id(ad):

    creative_id = None
    try:
        creative_id = ad['creative']['id']
    except KeyError:
        print 'creative Id not found'

    return creative_id


def main(start_date, end_date):
    """
        Fetch post ids and corresponding post data
    :param start_date:
    :param end_date:
    :return:
    """
    attempt = 1
    while attempt <= API_ATTEMPTS:
        try:

            params = CONFIG['params']

            # Add date range to params
            params['since'] = start_date
            params['until'] = end_date

            # Overwrite fields
            params['fields'] = ['id']
            params['limit'] = 100

            ids = []
            for end_point in END_POINTS:
                if end_point == 'posts':
                    token = fb_access_token
                else:
                    token = fb_page_access_token
                    # Get graph api
                graph_api = GraphAPI(access_token=token,
                                     app_secret=fb_app_secret)

                ids.extend(get_api_data(graph_api, end_point, params))
            # pp.pprint(raw_data)

            ### Get Ad Post Ids
            api = fb_modules.get_api(
                app_id=fb_app_id,
                app_secret=fb_app_secret,
                access_token=fb_access_token)
            FacebookAdsApi.set_default_api(api)

            # Setup user and read the object from the server
            me = AdAccountUser(fbid='me')
            # Get all accounts connected to the user
            accounts = fb_modules.get_accounts(me)
            ad_fields = CONFIG['ad_params']['fields']

            for account in accounts:
                # download only for social account
                if account['id'] == SOCIAL_ACCT:

                    print "Getting ads ..."
                    ads_data = fb_modules.to_dict(
                        fb_api_data=account.get_ads(params=CONFIG['ad_params']),
                        fields=ad_fields)

                    # Fetch creative id from add and get object story id
                    for ad in ads_data:
                        creative_id = get_creative_id(ad)

                        if creative_id is not None:
                            creative = AdCreative(creative_id)
                            creative_data = creative.remote_read(fields=[AdCreative.Field.name,
                                                                        AdCreative.Field.effective_object_story_id])
                            ids.append({'id': creative_data.get('effective_object_story_id')})

            ids_uniq = list(set([item.get('id') for item in ids if item.get('id')]))
            print "Fetched {0} number of ids".format(len(ids_uniq))

            post_data = []
            for id in ids_uniq:
                print "Fetching post data {0}".format(id)
                data = get_post_perf_data(graph_api, id)
                if data:
                    post_data.append(data)
                    time.sleep(1)
            break
        except Exception, e:
            print "Error: can not get the data via Facebook API."
            print str(e)
            if attempt == API_ATTEMPTS:
                print "Error: can't get the data via Facebook API after {} attempts".format(
                    attempt)
                sys.exit(1)
            attempt += 1
            time.sleep(TIMEOUT)

    # Create Redshift json file
    export_to_json_redshift(post_data, os.path.join(
       output_file_dir, CONFIG['dwh_file_name'] + '.json'))


if __name__ == '__main__':
    desc = (
        "Getting Facebook performance data through API call and convert results to json file."
    )
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '-c',
        '--config_file',
        required=True,
        action='store',
        dest='config_file',
        help='Config json file with "fields":[field1...] and/or "breakdowns":[field1...]')
    parser.add_argument(
        '-i',
        '--input_dir',
        required=True,
        action='store',
        dest='input_dir',
        help='Input directory')
    parser.add_argument(
        '-o',
        '--output_dir',
        required=True,
        action='store',
        dest='output_dir',
        help='Output directory')
    parser.add_argument(
        '-s',
        '--start_date',
        required=True,
        action='store',
        type=valid_date,
        dest='start_date',
        help='Start date in "yyyy-mm-dd" format')
    parser.add_argument(
        '-e',
        '--end_date',
        required=True,
        action='store',
        type=valid_date,
        dest='end_date',
        help='End date in "yyyy-mm-dd" format')
    parser.add_argument(
        '--fb_app_id',
        required=True,
        action='store',
        dest='fb_app_id',
        help='FB App ID')
    parser.add_argument(
        '--fb_app_secret',
        required=True,
        action='store',
        dest='fb_app_secret',
        help='FB App Secret')
    parser.add_argument(
        '--fb_access_token',
        required=True,
        action='store',
        dest='fb_access_token',
        help='FB App Access Token')
    parser.add_argument(
        '--fb_page_access_token',
        required=True,
        action='store',
        dest='fb_page_access_token',
        help='FB Page Access Token')

    args = parser.parse_args()

    start_date, end_date = args.start_date, args.end_date
    print "date_range: " + start_date + ',' + end_date

    # input file name e.g. config.json
    input_config_file_nm = args.config_file

    # output file location
    output_file_dir = args.output_dir

    # get FB credentials
    fb_app_id = args.fb_app_id
    fb_app_secret = args.fb_app_secret
    fb_access_token = args.fb_access_token
    fb_page_access_token = args.fb_page_access_token

    pp = pprint.PrettyPrinter(indent=4)
    this_dir = os.path.dirname(__file__)
    config_filename = os.path.join(this_dir, input_config_file_nm)

    # Read config file
    try:
        with open(config_filename) as config_file:
            CONFIG = json.load(config_file)
    except Exception, e:
        print "Error: can not open config file {}".format(config_filename)
        print str(e)
        sys.exit(1)

    # Do work
    main(start_date, end_date)
