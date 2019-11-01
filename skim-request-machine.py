import argparse
import httplib
import logging
from contextlib import closing
from datetime import datetime, timedelta


def date_to_str_with_pattern(dates_list, pattern="%Y-%m-%d"):
    return map(lambda d: d.strftime(pattern), dates_list)


def parse_dates(dates_str):
    def get_dates_between(start_date, finish_date):
        delta = finish_date - start_date
        days = range(delta.days + 1)
        return [start_date + timedelta(d) for d in days]

    def strip_and_parse_to_date(date_str):
        return datetime.strptime(date_str.strip(), '%d.%m.%Y')

    def date_generator(str_dates):
        for str_date in str_dates.split(','):
            if '-' in str_date:
                period = map(strip_and_parse_to_date, str_date.split('-'))
                yield get_dates_between(period[0], period[1])
            else:
                yield [strip_and_parse_to_date(str_date)]

    return reduce(lambda acc, element: acc + element, date_generator(dates_str))


def do_request(req_address, req_dates, req_datamart):
    try:
        with closing(httplib.HTTPConnection(req_address, timeout=1800)) as conn:
            logging.info("Open connection to {}".format(req_address))

            for req_date in req_dates:
                headers = {"ldate": req_date, "datamart": req_datamart}

                logging.info("Send request: date = {}, datamart = {}".format(req_date, datamart))
                conn.request("GET", "/skim/datamart/data-extractor", headers=headers)

                response = conn.getresponse()
                logging.info("Response status: {}. Reason: {}".format(str(response.status), response.reason))
                logging.info("Response body: {}".format(response.read()))

                if response.status != 200:
                    break
    except Exception as e:
        logging.fatal(e, exc_info=True)


if __name__ == "__main__":
    # setup argument parser
    argParser = argparse.ArgumentParser('Skim datamart request sender')

    group = argParser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", "--file", required=False, help="dates file")
    group.add_argument("-d", "--date", required=False, help="request date - format YYYY-MM-DD")

    argParser.add_argument("-a", "--address", required=True, help="host:port")
    argParser.add_argument("-dm", "--datamart", required=True, help="datamart_c or datamart_n")
    args = vars(argParser.parse_args())

    # setup logging to the file 'skim-request-machine.log' and console
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='skim-request-machine.log')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    address = args['address']
    filePath = args['file']
    date = args['date']
    datamart = args['datamart']

    if filePath is not None:
        logging.info("Open request file {}".format(filePath))
        with open(filePath, 'r') as fileHandler:
            content = fileHandler.read()

            logging.info("Parsing dates")
            datesList = parse_dates(content)
            dates = date_to_str_with_pattern(datesList)

            logging.info(dates)
    else:
        dates = [date]

    do_request(address, dates, datamart)
