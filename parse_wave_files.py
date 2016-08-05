#! /home/michaesm/.envs/parse_wave_files/bin/python
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import DOUBLE, TINYINT, TIMESTAMP, DECIMAL, VARCHAR, FLOAT, CHAR, INTEGER, SMALLINT
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
import datetime as dt
import re
import time
import glob
from configs import configs


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**database))


def check_site(site, freq=None, origin=None):
    """
    Check if site exists in database. If it doesn't, create it.
    :param ref_des: codar site string
    :return: id of codar site
    """
    site_dict = dict(site=site)
    result = ses.query(Sites).filter_by(**site_dict).first()

    if freq and origin:
        if not result:
            print "New HFR site detected. Updating table `hfrSites`"
            lonlat = re.findall(r"[-+]?\d*\.\d+|\d+", origin)
            new_site = dict(site=site, transmitCenterFrequency=freq, lat=lonlat[0], lon=lonlat[1])
            # new_site = dict(site=site)
            ref = Sites(**new_site)
            ses.add(ref)
            ses.commit()
            ses.flush()
            ref_id = ref.id
            # print "'%s' added to table `hfrSites`" % new_site[site]
            return ref_id
        else:
            return result.id
    else:
        return result.id


def parse_header_line(line):
    """
    Parse a line into a key, value
    :param line: a line from a text file
    :type line: string
    :return: a tuple containing the key, value for the line
    :rtype: tuple
    """

    line = line.replace('%', '') # Strip the % sign from the line
    line = line.replace('\n', '') # Strip the new line character from the end of the line
    key = line[0:line.find(':')] # Grab the key, which is everything to the left of the colon
    value = line[line.find(':') + 2:] # Grab the value, which is everything to the right of the colon
    return key, value


def parse_header(data):
    """
    Parse the header data into a dictionary
    :param data: .readlines() data from an open text file
    :return header_dict: dictionary containing all the important header information for each file
    :rtype: dictionary
    """

    m = 0
    header_dict = {} # intitialize an empty dictionary
    for line in data:
        if line.find('%', 0, 1) != -1:  # Find lines that start with a %
            if line.find('%%', 0, 2) != -1:  # Find lines that start with two %%.
                m = m + 1
                if m is 5:
                    break # Break the loop when the fifth %% is found
                else:
                    continue
            elif ('Distance' in line) or ('RangeCell' in line) or ('TableType' in line) or ('TableColumns' in line) or ('TableRows' in line) or ('TableStart' in line):
                continue # We don't care about any of this information in the header
            else:
                key, value = parse_header_line(line)
                header_dict[key] = value
    header_dict = clean_header(header_dict) # Clean the header data for input into MySQL database.
    return header_dict


def clean_header(head_dict):
    """
    Cleans the header data from the wave data for proper input into MySQL database
    :param head_dict: dictionary containing the header data
    :return: dictionary containing the cleaned header data
    :rtype:
    """

    for k, v in head_dict.iteritems():
        if 'Site' in k:
            head_dict[k] = ''.join(e for e in v if e.isalnum())
        elif 'TimeStamp' in k:
            t_list = v.split()
            t_list = [int(s) for s in t_list]
            head_dict[k] = dt.datetime(t_list[0], t_list[1], t_list[2], t_list[3], t_list[4], t_list[5]).strftime('%Y-%m-%d %H:%M:%S')
        elif ('TimeCoverage' in k) or ('RangeResolutionKMeters' in k):
            head_dict[k] = re.findall("\d+\.\d+", v)[0]
        elif ('WaveMergeMethod' in k) or ('WaveUseInnerBragg' in k) or ('WavesFollowTheWind' in k):
            head_dict[k] = re.search(r'\d+', v).group()
        elif 'TimeZone' in k:
            head_dict[k] = re.search('"(.*)"', v).group(1)
        elif ('WaveBearingLimits' in k) or ('CoastlineSector' in k):
            bearings = re.findall(r"[-+]?\d*\.\d+|\d+", v)
            head_dict[k] = ', '.join(e for e in bearings)
        else:
            continue
    return head_dict


def check_file_upload(f_name):
    """
    :param f_name: file name of the spreadsheet to be checked
    :return: returns true or false if the spreadsheet has already been uploaded before
    """
    data_dict = dict(filename=os.path.splitext(os.path.basename(f_name))[0])
    uploaded = ses.query(WaveFile).filter_by(**data_dict).first()
    return uploaded


def flag_bad_data(wvht):
    if 0.75 <= wvht <= 5:
        flag = 1
    else:
        flag = 4
    return flag


def iterate_through_data(data, header_keys, site_id, ref_id, bulk=True):
    """
    Loop through the data contained in the text file
    :param data: .readlines() data from an open text file
    :param header_keys: the column names in the proper order that will be uploaded.
    :param site_id:  id for the hfr site
    :param ref_id: id for the file metadata that was updated
    :param bulk: boolean whether to upload the entire file in bulk (completed wave files) or each line separately (for files still being written).
    :return: nothing
    """

    bulk_list = []
    n = 0
    for line in data:
        if '%' in line:
            continue
        else:
            line = line.replace('\n', '')
            line_dict = dict(zip(header_keys, line.split()))
            line_dict['site_id'] = int(site_id)
            line_dict['file_id'] = int(ref_id)
            line_dict['mwht_flag'] = int(flag_bad_data(line_dict['MWHT']))
            line_ref = WaveData(**line_dict)
            if not bulk:
                line_uploaded = ses.query(WaveData).filter_by(**line_dict).first()
                if line_uploaded:
                    continue
                print 'Uploading new row: %s' % str(line_ref)
                ses.add(line_ref)
                n = n + 1
            else:
                bulk_list.append(line_ref)

    if bulk:
        print 'Inserting data from entire wave file.'
        ses.bulk_save_objects(bulk_list)
        print '%d rows inserted.' % len(bulk_list)
    else:
        print '%d rows inserted.' % n
    ses.commit()
    ses.flush()


def parse_wave_file(fname):
    """
    Function to parse the wave file
    :param fname: directory/filename of the wave file
    :return: Nothing
    """
    file_name = os.path.splitext(os.path.basename(fname))[0]
    with open(fname) as wave_file:
        wave_data = wave_file.readlines()

        header_data = parse_header(wave_data)
        header_data['filename'] = file_name
        header_data['Site'] = check_site(header_data['Site'], header_data['TransmitCenterFreqMHz'], header_data['Origin'])

        ref = WaveFile(**header_data)
        ses.add(ref)
        ses.commit()
        ses.flush()
        ref_id = ref.id
        header_keys = header_data['TableColumnTypes'].split()
        iterate_through_data(wave_data, header_keys, header_data['Site'], ref_id, True)

database = configs.db_configs()

global e
e = db_connect()
Base = declarative_base()


class WaveFile(Base):
    __tablename__ = "hfrWaveFilesMetadata"

    id = Column(INTEGER, primary_key=True)
    filename = Column(VARCHAR)
    CTF = Column(DECIMAL)
    FileType = Column(VARCHAR)
    UUID = Column(VARCHAR)
    Manufacturer = Column(VARCHAR)
    Site = Column(INTEGER)
    TimeStamp = Column(TIMESTAMP)
    TimeZone = Column(CHAR)
    TimeCoverage = Column(SMALLINT)
    Origin = Column(VARCHAR)
    RangeCells = Column(SMALLINT)
    RangeResolutionKMeters = Column(FLOAT)
    AntennaBearing = Column(SMALLINT)
    TransmitCenterFreqMHz = Column(FLOAT)
    BraggSmoothingPoints = Column(SMALLINT)
    CurrentVelocityLimit = Column(SMALLINT)
    TransmitSweepRateHz = Column(FLOAT)
    TransmitBandwidthKHz = Column(FLOAT)
    TableColumnTypes = Column(VARCHAR)
    TableRows = Column(SMALLINT)
    CoastlineSector = Column(VARCHAR)
    DopplerCells = Column(FLOAT)
    MaximumWavePeriod = Column(FLOAT)
    WaveBearingLimits = Column(VARCHAR)
    WaveBraggNoiseThreshold = Column(FLOAT)
    WaveBraggPeakDropOff = Column(FLOAT)
    WaveBraggPeakNull = Column(FLOAT)
    WaveMergeMethod = Column(SMALLINT)
    WaveMinDopplerPoints = Column(SMALLINT)
    WaveUseInnerBragg = Column(SMALLINT)
    WavesFollowTheWind = Column(SMALLINT)
    BraggHasSecondOrder = Column(TINYINT)


class WaveData(Base):
    __tablename__ = "hfrWaveData"

    id = Column(INTEGER, primary_key=True)
    site_id = Column(SMALLINT)
    TIME = Column(INTEGER)
    MWHT = Column(DECIMAL)
    MWPD = Column(DECIMAL)
    WAVB = Column(DECIMAL)
    WNDB = Column(DECIMAL)
    ACNT = Column(TINYINT)
    DIST = Column(DECIMAL)
    RCLL = Column(TINYINT)
    WDPT = Column(TINYINT)
    MTHD = Column(TINYINT)
    FLAG = Column(TINYINT)
    TYRS = Column(SMALLINT)
    TMON = Column(TINYINT)
    TDAY = Column(TINYINT)
    THRS = Column(TINYINT)
    TMIN = Column(TINYINT)
    TSEC = Column(TINYINT)
    file_id = Column(SMALLINT)
    mwht_flag = Column(TINYINT)


class Sites(Base):
    __tablename__ = "hfrSites"

    id = Column(INTEGER, primary_key=True)
    site = Column(VARCHAR)
    lat = Column(DOUBLE)
    lon = Column(DOUBLE)
    transmitCenterFrequency = Column(DOUBLE)

Session = sessionmaker(bind=e)

global ses
ses = Session()

wave_dir = '/Volumes/arctic/codaradm/data/waves/'
sites = ['BRAD', 'BRMR', 'BRNT', 'RATH', 'SEAB', 'SPRK', 'WOOD',
         'BISL', 'CMPT', 'CPHN', 'GCAP', 'HLPN', 'MISQ', 'MNTK',
         'OLDB', 'PALM', 'PORT', 'SILD', 'STLI', 'SUNS', 'VIEW']

matches = []

for site in sites:
    site_dir = os.path.join(wave_dir, site)
    for fname in glob.glob(os.path.join(site_dir, '*.wls')):
        print fname
        uploaded = check_file_upload(fname)
        if not uploaded:
            print 'Parsing ' + fname
            parse_wave_file(fname)
            print fname + ' upload complete.'
        else:
            print "File uploaded already. Checking if file has been modified in the past 30 days."
            one_month_ago = dt.datetime.now() - dt.timedelta(days=30)
            mod_time = dt.datetime.utcfromtimestamp(os.path.getmtime(fname))
            print 'Thirty days ago: ' + str(one_month_ago)
            print fname + ' last modified on ' + str(mod_time)
            site_code = os.path.splitext(os.path.basename(fname))[0].split('_')[1]
            site_id = check_site(site_code)
            print "File last modified: %s" % time.ctime(os.path.getmtime(fname))

            if mod_time > one_month_ago:
                with open(fname) as wave_file:
                    wave_data = wave_file.readlines()
                    for line in wave_data:
                        if 'TableColumnTypes' in line:
                            split_line = parse_header_line(line)
                            header_keys = split_line[1].split()
                            break
                    iterate_through_data(wave_data, header_keys, site_id, uploaded.id, False)
            else:
                print 'File not modified within the past 30 days. Continuing to next wave file.'
                continue