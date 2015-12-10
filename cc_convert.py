# cc_convert - a script to convert UK Charity Commission data dumps to RDF or CSV
#     Copyright (C) 2015  Networked Planet Limited
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <http://www.gnu.org/licenses/>.

import csv
import argparse
import glob
import os
import datetime
import hashlib
import re


# The common set of CURIE prefix mappings for all of the generated Turtle files
PREFIXES = {
    's': 'http://schema.org/',
    'charity': 'http://data.networkedplanet.com/data/charity_commission/charity/',
    'ont': 'http://data.networkedplanet.com/data/charity_commission/ontology/',
    'reg': 'http://data.networkedplanet.com/data/charity_commission/ontology/registerStatus/',
    'rem': 'http://data.networkedplanet.com/data/charity_commission/ontology/removalReason/',
    'area': 'http://data.networkedplanet.com/data/charity_commision/ontology/area/',
    'class': 'http://data.networkedplanet.com/data/charity_commission/ontology/class/',
    'foaf': 'http://xmlns.com/foaf/0.1/',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    'owl': 'http://www.w3.org/2002/07/owl#',
    'oc': 'https://opencorporates.com/id/companies/gb/',
    'xsd': 'http://www.w3.org/2001/XMLSchema#'
}


class BcpParseError(Exception):
    def __init__(self, msg):
        super(BcpParseError, self).__init__(msg)


def parse_bcp(f):
    """ Incremental parse of a BCP data file.
    :param f: The TextIOBase instance to read the data from
    :return: yields an array for each row in the BCP data file. Each array contains a single value for each cell in thr row - either a string value or None

    >>> from io import BytesIO
    >>> list(parse_bcp(BytesIO('PENLLYN PARISH*@**@@**@F@**@*@@*'.encode('latin-1'))))
    [['PENLLYN PARISH*', None, 'F', None]]
    >>> list(parse_bcp(BytesIO('200000@**@0@**@1961-06-08 00:00:00@**@1998-02-04 16:28:00@**@CE *@@*'.encode('latin-1'))))
    [['200000', '0', '1961-06-08 00:00:00', '1998-02-04 16:28:00', 'CE']]
    >>> list(parse_bcp(BytesIO('200002@**@0@**@1961-06-08 00:00:00@**@@**@*@@*200003@**@0@**@1961-06-08 00:00:00@**@2009-09-09 01:06:00@**@NO *@@*'.encode('latin-1'))))
    [['200002', '0', '1961-06-08 00:00:00', None, None], ['200003', '0', '1961-06-08 00:00:00', '2009-09-09 01:06:00', 'NO']]
    >>> list(parse_bcp(BytesIO('201415@**@1@**@JOHN BAVERSTOCK@**@RM@**@FOUNDATION ABOUT 1830.;@**@@@**@@**@F@**@*@@*'.encode('latin-1'))))
    [['201415', '1', 'JOHN BAVERSTOCK', 'RM', 'FOUNDATION ABOUT 1830.;', '@', None, 'F', None]]
    >>> list(parse_bcp(BytesIO('217855@**@0@**@ALLOTMENTS FOR THE LABOURING POOR@**@R@**@REPORTS ON ENDOWED CHARITIES (GLAMORGAN) 1897. ENCLOSURE AWARD 1860 AND THE AMENDED AWARD OF 1862 AS VARIED BY SCHEME*;@**@PENLLYN PARISH@**@@**@F@**@*@@*'.encode('latin-1'))))
    [['217855', '0', 'ALLOTMENTS FOR THE LABOURING POOR', 'R', 'REPORTS ON ENDOWED CHARITIES (GLAMORGAN) 1897. ENCLOSURE AWARD 1860 AND THE AMENDED AWARD OF 1862 AS VARIED BY SCHEME*;', 'PENLLYN PARISH', None, 'F', None]]
    """
    row = []
    expected_cell_count = 0
    cell = ''
    pos = 0
    for ch in iter(lambda: f.read(1).decode('iso-8859-1'), ''):
        pos += 1
        if ch == '@' or ch == '*':
            # Use a three-character look-ahead to check if this is the start of a cell or row break
            current_offset = f.tell()
            buff = f.read(3).decode('iso-8859-1')
            if ch == '@' and buff == '**@':
                # Valid cell-break
                row.append(None if cell == '' else cell.rstrip())
                cell = ''
            elif ch == '*' and buff == '@@*':
                # Valid row-break
                row.append(None if cell == '' else cell.rstrip())
                cell = ''
                if expected_cell_count == 0:
                    expected_cell_count = len(row)
                elif expected_cell_count != len(row):
                    raise BcpParseError('Unexpected cell count on row. Expected {} cells, got {}. Currently at char {}'
                                        .format(expected_cell_count, len(row), pos))
                yield row
                row = []
            else:
                # Not a valid break
                f.seek(current_offset)
                cell = cell + ch
        else:
            cell = cell + ch


def bcp_to_csv(bcp_path, csv_path):
    """
    Parse the contents of a BCP file and write them out as a CSV file

    :param bcp_path: The path to the BCP input file to read from
    :param csv_path: The path to the CSV output file to write to. If the file exists it will be overwritten.
    :return: None
    """
    with open(bcp_path, 'rb') as bcp_file:
        with open(csv_path, 'w', newline='', encoding='utf8') as csv_file:
            data_writer = csv.writer(csv_file)
            row_count = 0
            for r in parse_bcp(bcp_file):
                data_writer.writerow(r)
                row_count += 1
                if row_count % 100 == 0:
                    print(row_count)
                if row_count % 10000 == 0:
                    csv_file.flush()


def parse_tsv(tsv_file):
    """
    Really simple TSV file reader.
    Due to the nature of the CC source data, this reader does not treat a CR character alone as a row terminator.

    :param tsv_file: The path to the TSV file to read from
    :return: An iterator that yields an array of values for each row in the TSV file
    """
    # Python csv reader doesn't like the use of CR inside a field
    # return csv.reader(tsv_file, dialect=csv.excel_tab)
    for line in tsv_file:
        yield [x.rstrip() for x in line.rstrip('\r\n').split('\t')]


def sir_to_csv(tsv_path, csv_path):
    with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        data_writer = csv.writer(csv_file)
        for r in parse_sir(tsv_path):
            data_writer.writerow(r)


def parse_sir(tsv_path):
    with open(tsv_path, 'r', encoding='iso-8859-1', newline='\r\n') as tsv_file:
        expected_cell_count = 0
        line_no = 0
        for r in parse_tsv(tsv_file):
            line_no += 1
            if expected_cell_count == 0:
                expected_cell_count = len(r)
            elif len(r) > expected_cell_count:
                # print('WARN: Merging overflow cells: {0}', r)
                merged_cell = '\t'.join(r[expected_cell_count - 1:])
                r = list(r[:expected_cell_count - 2])
                r.append(merged_cell)
            elif len(r) != expected_cell_count:
                print('ERROR: Invalid line (there may be a bad CR/LF pair in preceding cell data')
            yield r


def convert_to_csv(source_dir, target_dir):
    for src in glob.iglob(os.path.join(source_dir, '*.bcp')):
        csv_path = os.path.join(target_dir, os.path.basename(src) + '.csv')
        if os.path.exists(csv_path):
            print('Found existing CSV file at {}. Skipping conversion'.format(csv_path))
            continue
        if os.path.basename(src).startswith("sir_data"):
            print('Converting SIR data (TSV) {} to {}'.format(src, csv_path))
            sir_to_csv(src, csv_path)
        else:
            print('Converting BCP {} to {}'.format(src, csv_path))
            bcp_to_csv(src, csv_path)


def convert_to_rdf(source_dir, target_dir):
    turtle_conversion(
        os.path.join(source_dir, 'extract_acct_submit.bcp'),
        os.path.join(target_dir, 'acct_submit.ttl'),
        convert_account_submissions)

    turtle_conversion(
        os.path.join(source_dir, 'extract_aoo_ref.bcp'),
        os.path.join(target_dir, 'aoo_ref.ttl'),
        convert_aoo)

    turtle_conversion(
        os.path.join(source_dir, 'extract_ar_submit.bcp'),
        os.path.join(target_dir, 'ar_submit.ttl'),
        convert_ar_submissions)

    turtle_conversion(
        os.path.join(source_dir, 'extract_charity.bcp'),
        os.path.join(target_dir, 'charity.ttl'),
        convert_charities_extract)

    turtle_conversion(
        os.path.join(source_dir, 'extract_charity_aoo.bcp'),
        os.path.join(target_dir, 'charity_aoo.ttl'),
        convert_charity_aoo)

    turtle_conversion(
        os.path.join(source_dir, 'extract_class.bcp'),
        os.path.join(target_dir, 'class.ttl'),
        convert_class)

    turtle_conversion(
        os.path.join(source_dir, 'extract_class_ref.bcp'),
        os.path.join(target_dir, 'class_ref.ttl'),
        convert_class_ref)

    turtle_conversion(
        os.path.join(source_dir, 'extract_financial.bcp'),
        os.path.join(target_dir, 'financial.ttl'),
        convert_financial
    )

    turtle_conversion(
        os.path.join(source_dir, 'extract_main_charity.bcp'),
        os.path.join(target_dir, 'main_charity.ttl'),
        convert_main_charity)

    turtle_conversion(
        os.path.join(source_dir, 'extract_name.bcp'),
        os.path.join(target_dir, 'name.ttl'),
        convert_name)

    turtle_conversion(
        os.path.join(source_dir, 'extract_objects.bcp'),
        os.path.join(target_dir, 'objectives.ttl'),
        convert_objectives)

    turtle_conversion(
        os.path.join(source_dir, 'extract_partb.bcp'),
        os.path.join(target_dir, 'partb.ttl'),
        convert_partb
    )

    turtle_conversion(
        os.path.join(source_dir, 'extract_registration.bcp'),
        os.path.join(target_dir, 'registration.ttl'),
        convert_registration
    )

    turtle_conversion(
        os.path.join(source_dir, 'extract_remove_ref.bcp'),
        os.path.join(target_dir, 'remove_ref.ttl'),
        convert_removal_ref
    )

    # Omitted for privacy
    # turtle_conversion(
    #     os.path.join(source_dir, 'extract_trustee.bcp'),
    #     os.path.join(target_dir, 'trustee.ttl'),
    #     convert_trustee
    # )

    sir_to_rdf(
        os.path.join(source_dir, 'sir_data.bcp'),
        os.path.join(target_dir, 'sir_data.ttl'))


def write_prefixes(f):
    for prefix, uri in PREFIXES.items():
        f.write('@prefix {}: <{}> .\n'.format(prefix, uri))


def escape_string(to_escape):
    return to_escape.strip().replace('\\', '\\\\').replace('\t', '\\t').replace('\b', '\\b')\
        .replace('\n', '\\n').replace('\r', '\\r').replace('\f', '\\f').replace('\'', '\\\'').replace('\"', '\\"')


def charity_iri(registered_number, subsidiary_number):
    if subsidiary_number and subsidiary_number != '0':
        return '<{}{}/subsidiary/{}>'.format(PREFIXES['charity'], registered_number, subsidiary_number)
    else:
        return '<{}{}>'.format(PREFIXES['charity'], registered_number)


def datetime_to_date_iri(s, fmt="%Y-%m-%d %H:%M:%S"):
    dt = datetime.datetime.strptime(s.strip(), fmt)
    return dt.strftime("<http://reference.data.gov.uk/id/day/%Y-%m-%d>")


def parse_datetime(s, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.strptime(s.strip(), fmt)


def datetime_to_xsd_datetime(s, fmt="%Y-%m-%d %H:%M:%S"):
    """
    Parse s to a Python datetime object and then serialize it in the lexical representation
    specified by the XML Schema datatypes spec. Currently the whole thorny issue of timezone
    is obviated by omitting the timezone from the returned timestamp string!

    :param s:
    :param fmt:
    :return:
    """
    dt = parse_datetime(s, fmt)
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def join_continuation_strings(arry):
    if len(arry) == 1:
        return arry[0]
    for i in range(0, len(arry)-1):
        if arry[i][-4:] == '0001':
            arry[i] = arry[i][:-4]
    return ''.join(arry)


def sir_to_rdf(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    print('Convert from {} to {}'.format(bcp_path, rdf_path))
    last_id = None
    questions_described = {}
    with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
        write_prefixes(rdf_file)
        for row in parse_sir(bcp_path):
            if len(row) < 6 or len(row) > 7:
                print('Skipping row with unexpected number of columns: {} (expected 6 or 7)'.format(len(row)))
                continue
            charity_id = charity_iri(row[0], '0')
            # charity_name = row[1] -- NOT USED HERE
            return_cycle = row[2]
            return_cycle_id = '<{}sirCycle/{}>'.format(PREFIXES['charity'], row[2])
            question_number = row[3]
            question_id = '<{}/summaryInformationQuestion/{}/{}>'.format(PREFIXES['charity'], return_cycle, question_number)
            return_id = '<{}{}/summaryInformationReturn/{}>'.format(PREFIXES['charity'], row[0], return_cycle)
            response_id = '<{}{}/summaryInformationResponse/{}/{}>'.format(PREFIXES['charity'], row[0], return_cycle, question_number)
            if return_id != last_id:
                rdf_file.write('{} a ont:SummaryInformationReturn\n'.format(return_id))
                rdf_file.write('\t; ont:submittedBy {}\n'.format(charity_id))
                rdf_file.write('\t; ont:sirCycle {}\n'.format(return_cycle_id))
                rdf_file.write('\t.\n')
            if question_number not in questions_described:
                rdf_file.write('{} a ont:SummaryInformationQuestion\n'.format(question_id))
                rdf_file.write('\t;ont:questionText "{}"\n'.format(escape_string(row[4])))
                rdf_file.write('\t.\n')
            rdf_file.write('{} a ont:SummaryInformationResponse\n'.format(response_id))
            rdf_file.write('\t;ont:question {}\n'.format(question_id))
            if len(row[5]) > 0:
                if len(row) == 6:
                    rdf_file.write('\t;ont:responseText "{}"\n'.format(escape_string(row[5])))
                else:
                    rdf_file.write('\t;ont:responseText "{}"\n'.format(escape_string(row[5] + '\r' + row[6])))
            else:
                rdf_file.write('\t;ont:responseText "{}"\n'.format(escape_string(row[6])))
            rdf_file.write('\t.\n')


def turtle_conversion(bcp_path, rdf_path, conversion_function):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            print('Convert from {} to {}'.format(bcp_path, rdf_path))
            write_prefixes(rdf_file)
            conversion_function(bcp_file, rdf_file)


def convert_charities_extract(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        is_sub = row[1] != '0'
        if is_sub:
            s = '<{}{}/subsidiary/{}>'.format(PREFIXES['charity'], row[0], row[1])
            rdf_file.write('charity:{} s:childOrganization {} .\n'.format(row[0], s))
            rdf_file.write('{} a s:Organization, ont:Charity\n'.format(s))
            rdf_file.write('\t; s:parentOrganization charity:{}\n'.format(row[0]))
        else:
            s = 'charity:{}'.format(row[0])
            rdf_file.write('{} a s:Organization, ont:Charity\n'.format(s, row[0]))
        if row[2]:
            rdf_file.write('\t; s:name "{}"\n'.format(escape_string(row[2])))
        if row[3]:
            rdf_file.write('\t; ont:registerStatus reg:{}\n'.format(
                'REGISTERED' if row[3] == 'R' else 'REMOVED'))
        if row[4]:
            rdf_file.write('\t; ont:governingDocument "{}"\n'.format(escape_string(row[4])))
        if row[5]:
            rdf_file.write('\t; ont:areaOfBenefit "{}"\n'.format(escape_string(row[5])))
        if row[7] == 'T':
            rdf_file.write('\t; a ont:NhsCharity\n')
        if row[8]:
            rdf_file.write('\t; ont:housingAssociationNumber "{}"\n'.format(escape_string(row[8])))
        # Contact details omitted for privacy
        # if row[9]:
        #     rdf_file.write('\t; ont:contactName "{}"\n'.format(escape_string(row[9])))
        # if row[10] or row[15] or row[16] or row[17]:
        #     if is_sub:
        #         a = '<{}{}/subsidiary/{}/registeredAddress>'.format(PREFIXES['charity'], row[0], row[1])
        #     else:
        #         a = '<{}{}/registeredAddress>'.format(PREFIXES['charity'], row[0])
        #     rdf_file.write('\t; s:address {}\n'.format(a))
        #     rdf_file.write('\t.\n')
        #     street_address = '\n'.join(filter(lambda x: x, row[10:14]))
        #     rdf_file.write('{} s:streetAddress "{}" .\n'.format(a, escape_string(street_address)))
        #     if row[15]:
        #         rdf_file.write('{} s:postalCode "{}" .\n'.format(a, escape_string(row[15])))
        #     if row[16]:
        #         rdf_file.write('{} s:telephone "{}" .\n'.format(a, escape_string(row[16])))
        #     if row[17]:
        #         rdf_file.write('{} s:faxNumber "{}" .\n'.format(a, escape_string(row[17])))
        rdf_file.write('\t.\n')
        rdf_file.flush()


def convert_account_submissions(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        if all(row):
            # TODO: Change this so that the submission date and cycle are just properties of the FinancialAccounts resource ?
            acct_id = '<{}{}/accounts/{}>'.format(PREFIXES['charity'], row[0], row[2])
            submit_id = '<{}{}/accounts/{}/submission>'.format(PREFIXES['charity'], row[0], row[2])
            cycle_id = '<{}returnCycle/{}>'.format(PREFIXES['charity'], row[2])
            rdf_file.write('{} a ont:AccountsSubmission\n'.format(submit_id))
            rdf_file.write('\t; ont:submissionDate {}'.format(datetime_to_date_iri(row[1])))
            rdf_file.write('\t; ont:submissionTimestamp "{}"^^xsd:dateTime'.format(datetime_to_xsd_datetime(row[1])))
            rdf_file.write('\t; ont:submittedAccounts {}.\n'.format(acct_id))
            rdf_file.write('{} a ont:FinancialAccounts\n'.format(acct_id))
            rdf_file.write('\t; ont:returnCycle {}\n'.format(cycle_id))
            rdf_file.write('\t.\n')


def convert_ar_submissions(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        if all(row):
            # TODO: Change this so that the submission date and cycle are just properties of the AnnualReturn resource ?
            return_id = '<{}{}/annualReturn/{}>'.format(PREFIXES['charity'], row[0], row[1])
            submit_id = '<{}{}/annualReturn/{}/submission>'.format(PREFIXES['charity'], row[0], row[1])
            cycle_id = '<{}returnCycle/{}>'.format(PREFIXES['charity'], row[1])
            rdf_file.write('{} a ont:AnnualReturnSubmission\n'.format(submit_id))
            rdf_file.write('\t; ont:submissionDate {}\n'.format(datetime_to_date_iri(row[2])))
            rdf_file.write('\t; ont:submissionTimestamp "{}"^^xsd:dateTime\n'.format(datetime_to_xsd_datetime(row[2])))
            rdf_file.write('\t; ont:submittedReturn {} .\n'.format(return_id))
            rdf_file.write('{} a ont:AnnualReturn\n'.format(return_id))
            rdf_file.write('\t; ont:returnCycle {}\n'.format(cycle_id))
            rdf_file.write('\t;.\n')


def convert_aoo(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        area_id = "area:{}{}".format(row[0], row[1])
        name = row[2]
        desc = row[3]
        if row[0] == 'D':
            schema_type = "s:Country"
        else:
            schema_type = 's:AdministrativeArea'
        rdf_file.write("{} a ont:Area, {}\n".format(area_id, schema_type))
        if name:
            rdf_file.write('\t; rdfs:label "{}"\n'.format(escape_string(name)))
            rdf_file.write('\t; s:name "{}"\n'.format(escape_string(name)))
        if desc:
            rdf_file.write('\t; s:description "{}"'.format(escape_string(desc)))
        rdf_file.write('\t.\n')


def convert_charity_aoo(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity = "charity:" + row[0]
        area_type = row[1]
        area = "area:{}{}".format(row[1], row[2])
        master = row[4]
        rdf_file.write("{} ont:areaOfOperation {}.\n".format(charity, area))
        if master is not None:
            if area_type == 'D':
                # Master hold continent ref
                master_id = "area:E{}".format(master)
                rdf_file.write('{} s:containedInPlace {}.\n'.format(area, master_id))
            if area_type == 'B':
                # Master holds GLA/county
                master_id = 'area:C{}'.format(master)
                rdf_file.write('{} s:containedInPlace {}.\n'.format(area, master_id))


def convert_class(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity = "charity:" + row[0]
        cls = "class:" + row[1]
        rdf_file.write('{} ont:charitablePurpose {} .\n'.format(charity, cls))


def convert_class_ref(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        cls = "class:" + row[0]
        label = escape_string(row[1])
        rdf_file.write('{} a ont:CharitablePurposeClass\n'.format(cls))
        rdf_file.write('\t; rdfs:label "{}"\n'.format(cls, label))
        rdf_file.write('\t.\n')


def convert_financial(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity = charity_iri(row[0], '0')
        fystart = datetime_to_date_iri(row[1])
        fyend = datetime_to_date_iri(row[2])
        fyend_dt = parse_datetime(row[2])
        fystart_timestamp = datetime_to_xsd_datetime(row[1])
        fyend_timestamp = datetime_to_xsd_datetime(row[2])
        income = row[3]
        expenditure = row[4]
        summary_id = '<{}{}/financialSummary/{}>'.format(PREFIXES['charity'], row[0], fyend_dt.year)
        rdf_file.write('{} a ont:FinancialSummary\n'.format(summary_id))
        rdf_file.write('\t; ont:financialYearStart {}\n'.format(fystart))
        rdf_file.write('\t; ont:financialYearStartTimestamp "{}"^^xsd:dateTime\n'.format(fystart_timestamp))
        rdf_file.write('\t; ont:financialYearEnd {}\n'.format(fyend))
        rdf_file.write('\t; ont:financialYearEndTimestamp "{}"^^xsd:dateTime\n'.format(fyend_timestamp))
        rdf_file.write('\t; ont:income "{}"^^xsd:integer\n'.format(income))
        rdf_file.write('\t; ont:expenditure "{}"^^xsd:integer\n'.format(expenditure))
        rdf_file.write('\t.\n')
        rdf_file.write('{} ont:financialSummary {}.\n'.format(charity, summary_id))


def convert_main_charity(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity = "charity:" + row[0]
        company_no = row[1]
        if row[3]:
            fyearend_day = row[3][:2]
            fyearend_month = row[3][2:]
        else:
            fyearend_day = fyearend_month = None
        # income_date = datetime_to_date_iri(row[5])
        # income = int(row[6])
        # group_type = row[7]
        email = row[8]
        web = row[9]
        rdf_file.write('{} a ont:Charity\n'.format(charity))
        if company_no:
            # Declare sameAs the OpenCorporates resource for the company registration
            if re.fullmatch('\d+', company_no):
                rdf_file.write('\t; owl:sameAs oc:{}\n'.format(escape_string(company_no.strip())))
        if fyearend_day:
            rdf_file.write('\t; ont:financialYearEndDayOfMonth {}\n'.format(fyearend_day))
        if fyearend_month:
            rdf_file.write('\t; ont:financialYearEndMonthOfYear {}\n'.format(fyearend_month))
        if email:
            # For privacy we only publish a sha1 hash of the contact email address
            to_hash = "mailto:" + email.strip()
            mbox_sha1sum = hashlib.sha1(to_hash.encode('utf-8')).hexdigest()
            rdf_file.write('\t; foaf:mbox_sha1sum "{}"\n'.format(escape_string(mbox_sha1sum)))
        if web:
            rdf_file.write('\t; s:url "{}"^^s:URL\n'.format(escape_string(web)))
        rdf_file.write('\t.\n')


def convert_charity(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity = charity_iri(row[0], row[1])
        name = row[2]
        status = 'reg:{}\n'.format('REGISTERED' if row[3] == 'R' else 'REMOVED')
        governing_document = row[4]
        area_of_benefit = row[5]
        area_of_benefit_defined = (row[6] == 'T')
        nhs_charity = (row[7] == 'T')
        housing_association_number = row[8]
        # Correspondence name/address omitted for privacy
        postcode = row[15]
        # Phone/fax numbers omitted for privacy
        rdf_file.write('{} a ont:Charity\n'.format(charity))
        if nhs_charity:
            rdf_file.write('\t; a ont:NhsCharity\n')
        if housing_association_number:
            rdf_file.write('\t; a ont:HousingAssociation\n')
            rdf_file.write('\t; ont:housingAssociationNumber "{}"\n'.format(housing_association_number))
        if name:
            rdf_file.write('\t; rdfs:label "{}"'.format(escape_string(name)))
            rdf_file.write('\t; s:name "{}"'.format(escape_string(name)))
        rdf_file.write('\t; ont:registerStatus {}'.format(status))
        if governing_document:
            rdf_file.write('\t; ont:governingDocument "{}"'.format(escape_string(governing_document)))
        if area_of_benefit:
            rdf_file.write('\t;')


def convert_name(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity = charity_iri(row[0], row[1])
        name = escape_string(row[3])
        rdf_file.write('{0} rdfs:label "{1}"; s:name "{1}" .\n'.format(charity, name))


def convert_objectives(bcp_file, rdf_file):
    o = []
    current_charity = None
    for row in parse_bcp(bcp_file):
        charity = charity_iri(row[0], row[1])
        if charity != current_charity:
            if len(o):
                rdf_file.write('{} ont:objective "{}" .\n'.format(
                    current_charity, escape_string(join_continuation_strings(o))))
            current_charity = charity
            o = []
        ix = int(row[2])
        while len(o) < ix+1:
            o.append('')
        o[ix] = row[3]
    if len(o):
        rdf_file.write('{} ont:objective "{}" .\n'.format(
            current_charity, escape_string(join_continuation_strings(o))))


def convert_partb(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        return_id = '<{}{}/annualReturn/{}>'.format(PREFIXES['charity'], row[0], row[1])
        charity_id = charity_iri(row[0], '0')
        fystart_dt = datetime_to_xsd_datetime(row[2], '%Y-%m-%d %H:%M:%S.%f')
        fyend_dt = datetime_to_xsd_datetime(row[3], '%Y-%m-%d %H:%M:%S.%f')
        fystart = datetime_to_date_iri(row[2], '%Y-%m-%d %H:%M:%S.%f')
        fyend = datetime_to_date_iri(row[3], '%Y-%m-%d %H:%M:%S.%f')
        # Although the table schema says that all the remaining columns are varchar(max),
        # in practice the values are integers and so are presented as such in the RDF
        fin_stats = [
            'ont:legaciesIncome',
            'ont:endowmentsIncome',
            'ont:voluntaryIncome',
            'ont:activitiesGeneratingFunds',
            'ont:charitableActivitiesIncome',
            'ont:investmentIncome',
            'ont:otherIncome',
            'ont:totalIncome',
            'ont:investmentGain',
            'ont:assetGain',
            'ont:pensionGain',
            'ont:voluntaryIncomeCosts',
            'ont:fundraisingTradingCosts',
            'ont:investmentManagementCosts',
            'ont:grantsToInstitutions',
            'ont:charitableActivitiesCosts',
            'ont:governanceCosts',
            'ont:otherExpenditure',
            'ont:totalExpenditure',
            'ont:supportCosts',
            'ont:depreciation',
            'ont:reserves',
            'ont:fixedAssetsYearStart',
            'ont:fixedAssets',
            'ont:fixedInvestmentsAssets',
            'ont:fixedInvestmentsAssetsYearStart',
            'ont:currentInvestmentsAssets',
            'ont:cashAssets',
            'ont:currentAssets',
            'ont:creditors',
            'ont:longTermCreditors',
            'ont:pensionAssets',
            'ont:totalAssets',
            'ont:endowmentFunds',
            'ont:restrictedFunds',
            'ont:unrestrictedFunds',
            'ont:totalFunds',
            'ont:employees',
            'ont:volunteers'
        ]
        rdf_file.write('{} a ont:AnnualReturn\n'.format(return_id))
        rdf_file.write('\t; ont:submittedBy {}\n'.format(charity_id))
        rdf_file.write('\t; ont:financialYearStart {}\n'.format(fystart))
        rdf_file.write('\t; ont:financialYearStartTimestamp "{}"^^xsd:dateTime\n'.format(fystart_dt))
        rdf_file.write('\t; ont:financialYearEnd {}\n'.format(fyend))
        rdf_file.write('\t; ont:financialYearEndTimestamp "{}"^^xsd:dateTime\n'.format(fyend_dt))
        for i in range(4, len(fin_stats) + 4):
            if row[i]:
                rdf_file.write('\t; {} {}\n'.format(fin_stats[i-4], row[i]))
        is_consolidated_accounts = (row[42] == 'T')
        is_charity_only_accounts = (row[43] == 'T')
        rdf_file.write('\t; ont:consolidatedAccounts ' + ('true' if is_consolidated_accounts else 'false'))
        rdf_file.write('\n\t; ont:charityOnlyAccounts ' + ('true' if is_charity_only_accounts else 'false'))
        rdf_file.write('\n\t.\n')


def convert_registration(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity_id = charity_iri(row[0], row[1])
        reg_date = datetime_to_date_iri(row[2])
        reg_date_dt = datetime_to_xsd_datetime(row[2])
        rem_date = datetime_to_date_iri(row[3]) if row[3] else None
        rem_date_dt = datetime_to_xsd_datetime(row[3]) if row[3] else None
        rem_code = row[4]
        rdf_file.write('{} a ont:Charity\n'.format(charity_id))
        rdf_file.write('\t; ont:registrationDate {}\n'.format(reg_date))
        rdf_file.write('\t; ont:registrationDateTimestamp "{}"^^xsd:dateTime\n'.format(reg_date_dt))
        if rem_date is not None:
            rdf_file.write('\t; ont:removalDate {}\n'.format(rem_date))
            rdf_file.write('\t; ont:removalDateTimestamp "{}"^^xsd:dateTime\n'.format(rem_date_dt))
            if rem_code is not None:
                rdf_file.write('\t; ont:removalReason rem:{}\n'.format(rem_code))
        rdf_file.write('\t.\n')


def convert_removal_ref(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        rem_code = row[0]
        rem_text = row[1]
        rdf_file.write('rem:{} a ont:RemovalReason\n'.format(rem_code))
        rdf_file.write('\t;rdfs:label "{}"'.format(rem_text))
        rdf_file.write('\t.\n')


def convert_trustee(bcp_file, rdf_file):
    for row in parse_bcp(bcp_file):
        charity_id = charity_iri(row[0], '0')
        trustee_name = row[1]
        rdf_file.write('{} ont:trustee "{}".\n'.format(charity_id, escape_string(trustee_name)))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Convert Charity Commission data dump to CSV or RDF')
    p.add_argument('output', choices=['rdf', 'csv'])
    p.add_argument('source_dir')
    p.add_argument('target_dir')
    opts = p.parse_args()
    if not os.path.exists(opts.target_dir):
        os.makedirs(opts.target_dir)
    if opts.output == 'csv':
        convert_to_csv(opts.source_dir, opts.target_dir)
    else:
        convert_to_rdf(opts.source_dir, opts.target_dir)