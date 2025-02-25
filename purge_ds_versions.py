#!/usr/bin/env python3

"""Script for deleting older versions of specified datastream IDs from Fedora Commons 3.x"""

__author__ = "Paul Trilsbeek"
__license__ = "GPL3"
__version__ = "0.1"
import requests
import json
import logging
from lxml import etree as etree
from argparse import ArgumentParser

# global variables
FEDORA_URL = 'http://localhost:8080/fedora/objects/'
RISEARCH_URL = 'http://localhost:8080/fedora/risearch'
FEDORA_USER = 'fedoraAdmin'
FEDORA_PASS = 'fedora'

# input arguments
parser = ArgumentParser()
parser.add_argument("-d", "--dsids", dest="dsids", nargs=1, help="DSIDs for which to delete older verions. If more than one, separate with a comma (no spaces).", required=True)
parser.add_argument("-r", "--root", dest="root", nargs=1, help="The Fedora PID of the collection to use as the root of the search query (without info:fedora/).", required=True)
parser.add_argument("-k", "--keep", dest="keep", nargs=1, help="Number of versions to keep. Default = 1.", required=False)
args = parser.parse_args()
if args.dsids:
  dsids = args.dsids[0].split(",")
if args.root:
  root = args.root[0]
if args.keep:
  keep = int(args.keep[0])
else:
  keep = 1

# create logger
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
CH = logging.StreamHandler()
CH.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
CH.setFormatter(FORMATTER)
LOGGER.addHandler(CH)

# start HTTP sessions
fedora_session = requests.Session()
risearch_session = requests.Session()

# function for getting version history for a given pid and dsid. Returns dictionary with version number as key and date as value.
def get_versions(pid, dsid, fedora_url, fedora_user, fedora_pass, fedora_session, logger):
    result={}
    # fetch version histroy as XML using the Fedora API
    try:
        url = fedora_url + pid + "/datastreams/" + dsid + "/history?format=xml"
        response = fedora_session.get(url, auth=(fedora_user, fedora_pass))
        status = response.status_code
        if status == 200:
            response.encoding = 'utf-8'
            xml_content = response.text
            xml_root = etree.fromstring(bytes(xml_content, encoding='utf-8'))
            xml_tree = etree.ElementTree(xml_root)
            # get the versions from the XML
            versions = xml_tree.xpath('.//default:datastreamProfile', namespaces={'default': 'http://www.fedora.info/definitions/1/0/management/'})
            for version in versions:
                # get version id and date for each version
                version_id = version.xpath('.//default:dsVersionID', namespaces={'default': 'http://www.fedora.info/definitions/1/0/management/'})
                version_number = int(version_id[0].text.split(".",1)[1])
                version_date = version.xpath('.//default:dsCreateDate', namespaces={'default': 'http://www.fedora.info/definitions/1/0/management/'})
                result[version_number] = version_date[0].text
            return result
        else:
            logger.error(str(status) + ": Failed to get version history for pid  " + pid + " dsid " + dsid)
    except Exception as ex:
        logger.error(ex)

# function to purge all but "versions_to_keep" versions of a given pid and dsid
def purge_versions(pid, dsid, versions_to_keep, fedora_url, fedora_user, fedora_pass, fedora_session, logger):
    versions = get_versions(pid, dsid, fedora_url, fedora_user, fedora_pass, fedora_session, logger)
    number_of_versions = len(versions)
    if (number_of_versions > versions_to_keep):
        # get date of last version to purge, to be used as "endDT" parameter
        last_version_to_purge_key = list(versions.keys())[versions_to_keep]
        purge_date = versions[last_version_to_purge_key]
        try:
            url = fedora_url + pid + "/datastreams/" + dsid + "?endDT=" + purge_date
            response = fedora_session.delete(url, auth=(fedora_user, fedora_pass))
            status = response.status_code
            if status == 200:
                text = response.text
                logger.info("Purged versions for pid " + pid + " dsid " + dsid + ": " + text)
            else:
                logger.error(str(status) + ": Failed to purge versions for pid  " + pid + " dsid " + dsid)
        except Exception as ex:
            logger.error(ex)
    else:
        logger.info("No " + dsid + " versions to purge")

# function to query resource index for all objects that have a ds with given dsid, within a given pid as the root
def get_objects(dsid, root, risearch_url, fedora_user, fedora_pass, risearch_session, logger):
    querystring = 'SELECT DISTINCT ?x WHERE {\
    ?x <info:fedora/fedora-system:def/relations-external#isMemberOfCollection>+ <info:fedora/' + root + '> . \
    ?x <info:fedora/fedora-system:def/view#disseminates> ?ds . \
    filter contains(str(?ds),"' + dsid + '") }'

    query = {
        'query': querystring,
        'format': 'json',
        'type': 'tuples',
        'lang': 'sparql'
    }

    response = risearch_session.post(risearch_url, params=query, auth=(fedora_user, fedora_pass))
    result = []
    if response.ok:
        # Get the pids out of the response json
        result_obj = json.loads(response.text)
        for resultx in result_obj['results']:
           pid = resultx['x']
           pid = pid.replace('info:fedora/', '')
           result.append(pid)
    else:
      logger.error("There was an error querying the Resource Index.")
    return result


# main part
dsidcounter = 0

number_dsids = len(dsids)

LOGGER.info("root: " + root)

for dsid in dsids:

  dsidcounter += 1

  LOGGER.info(str(dsidcounter) + "/" + str(number_dsids) + " dsid: " + dsid)

  # get all pids underneath the specified root that have a ds with the given dsid
  pids = get_objects(dsid, root, RISEARCH_URL, FEDORA_USER, FEDORA_PASS, risearch_session, LOGGER)

  # for each pid, remove all except the latest "keep" versions of the given dsid
  if pids:

    number_pids = len(pids)

    LOGGER.info(str(number_pids) + " objects found.")
    LOGGER.info("keeping max. " + str(keep) + " version(s) of dsid " + dsid)

    pidcounter = 0

    for pid in pids:
      pidcounter += 1
      LOGGER.info("dsids: " + str(dsidcounter) + "/" + str(number_dsids) + " pids: " + str(pidcounter) + "/" + str(number_pids))
      LOGGER.info("processing object: " + pid)
      purge_versions(pid, dsid, keep, FEDORA_URL, FEDORA_USER, FEDORA_PASS, fedora_session, LOGGER)

  else:
    LOGGER.info("No objects found with dsid " + dsid)

LOGGER.info("Done.")