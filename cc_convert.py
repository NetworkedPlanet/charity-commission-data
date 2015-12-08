import csv
import argparse
import glob
import os
import datetime
import hashlib

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
    [['200000', '0', '1961-06-08 00:00:00', '1998-02-04 16:28:00', 'CE ']]
    >>> list(parse_bcp(BytesIO('200002@**@0@**@1961-06-08 00:00:00@**@@**@*@@*200003@**@0@**@1961-06-08 00:00:00@**@2009-09-09 01:06:00@**@NO *@@*'.encode('latin-1'))))
    [['200002', '0', '1961-06-08 00:00:00', None, None], ['200003', '0', '1961-06-08 00:00:00', '2009-09-09 01:06:00', 'NO ']]
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
                    raise BcpParseError("Unexpected cell count on row. Expected {} cells, got {}. Currently at char {}"
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
                    print("Flushing file...")
                    csv_file.flush()


def parse_tsv(tsv_file):
    # Python csv reader doesn't like the use of CR inside a field
    #return csv.reader(tsv_file, dialect=csv.excel_tab)
    for line in tsv_file:
        yield [x.rstrip() for x in  line.rstrip('\r\n').split('\t')]


def sir_to_csv(tsv_path, csv_path):
    with open(tsv_path, 'r', encoding='iso-8859-1', newline='\r\n') as tsv_file:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            data_writer = csv.writer(csv_file)
            expected_cell_count = 0
            line_no = 0
            for r in parse_tsv(tsv_file):
                line_no += 1
                if expected_cell_count == 0:
                    expected_cell_count = len(r)
                elif len(r) > expected_cell_count:
                    print('WARN: Merging overflow cells: {0}', r)
                    merged_cell = '\t'.join(r[expected_cell_count - 1:])
                    r = list(r[:expected_cell_count - 2])
                    r.append(merged_cell)
                elif len(r) != expected_cell_count:
                    print('ERROR: Invalid line (there may be a bad CR/LF pair in preceeding cell data')
                data_writer.writerow(r)


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
    convert_charities_extract(os.path.join(source_dir, 'extract_charity.bcp'),
                              os.path.join(target_dir, 'charity.ttl'))
    convert_account_submissions(os.path.join(source_dir, 'extract_acct_submit.bcp'),
                                os.path.join(target_dir, 'acct_submit.ttl'))
    convert_aoo(os.path.join(source_dir, 'extract_aoo_ref.bcp'),
                os.path.join(target_dir, 'aoo_ref.ttl'))
    convert_charity_aoo(os.path.join(source_dir, 'extract_charity_aoo.bcp'),
                        os.path.join(target_dir, 'charity_aoo.ttl'))
    convert_class(os.path.join(source_dir, 'extract_class.bcp'), os.path.join(target_dir, 'class.ttl'))
    convert_class_ref(os.path.join(source_dir, 'extract_class_ref.bcp'), os.path.join(target_dir, 'class_ref.ttl'))
    convert_main_charity(os.path.join(source_dir, 'extract_main_charity.bcp'),
                         os.path.join(target_dir, 'main_charity.ttl'))
    convert_name(os.path.join(source_dir, 'extract_name.bcp'),
                 os.path.join(target_dir, 'name.ttl'))
    convert_objectives(os.path.join(source_dir, 'extract_objects.bcp'),
                       os.path.join(target_dir, 'objectives.ttl'))

PREFIXES = {'s': 'http://schema.org/',
            'charity': 'http://data.networkedplanet.com/data/charity_commission/charity/',
            'ont': 'http://data.networkedplanet.com/data/charity_commission/ontology/',
            'reg': 'http://data.networkedplanet.com/data/charity_commission/ontology/registerStatus/',
            'area': 'http://data.networkedplanet.com/data/charity_commision/ontology/area/',
            'class': 'http://data.networkedplanet.com/data/charity_commission/ontology/class/',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#'}


def write_prefixes(f):
    for p, u in PREFIXES.items():
        f.write('@prefix {}: <{}>\n'.format(p, u))


def escape_string(to_escape):
    return to_escape.strip().replace('\\', '\\\\').replace('\t', '\\t').replace('\b', '\\b')\
        .replace('\n', '\\n').replace('\r', '\\r').replace('\f', '\\f').replace('\'', '\\\'').replace('\"', '\\"')


def charity_iri(regno, subno):
    if subno and subno != '0':
        return '<{}{}/subsidiary/{}>'.format(PREFIXES['charity'], regno, subno)
    else:
        return '<{}{}>'.format(PREFIXES['charity'], regno)


def convert_charities_extract(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
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
                    rdf_file.write('\t; ont:registerStatus reg:{}\n'.format('REGISTERED' if row[3] == 'R' else 'REMOVED'))
                if row[4]:
                    rdf_file.write('\t; ont:governingDocument "{}"\n'.format(escape_string(row[4])))
                if row[5]:
                    rdf_file.write('\t; ont:areaOfBenefit "{}"\n'.format(escape_string(row[5])))
                if row[9]:
                    rdf_file.write('\t; ont:contactName "{}"\n'.format(escape_string(row[9])))
                if row[10] or row[15] or row[16] or row[17]:
                    if is_sub:
                        a = '<{}{}/subsidiary/{}/registeredAddress>'.format(PREFIXES['charity'], row[0], row[1])
                    else:
                        a = '<{}{}/registeredAddress>'.format(PREFIXES['charity'], row[0])
                    rdf_file.write('\t; s:address {}\n'.format(a))
                    rdf_file.write('\t.\n')
                    street_address = '\n'.join(filter(lambda x: x, row[10:14]))
                    rdf_file.write('{} s:streetAddress "{}" .\n'.format(a, escape_string(street_address)))
                    if row[15]:
                        rdf_file.write('{} s:postalCode "{}" .\n'.format(a, escape_string(row[15])))
                    if row[16]:
                        rdf_file.write('{} s:telephone "{}" .\n'.format(a, escape_string(row[16])))
                    if row[17]:
                        rdf_file.write('{} s:faxNumber "{}" .\n'.format(a, escape_string(row[17])))
                rdf_file.write('\t.\n')
                rdf_file.flush()


def datetime_to_date_iri(s, fmt="%Y-%m-%d %H:%M:%S"):
    dt = datetime.datetime.strptime(s.strip(), fmt)
    return dt.strftime("<http://reference.data.gov.uk/id/day/%Y-%m-%d>")


def date_to_iri(s, fmt="%Y-%m-%d"):
    try:
        dt = datetime.datetime.strptime(s.strip(), fmt)
        return dt.strftime("<http://reference.data.gov.uk/id/day/%Y-%m-%d>")
    except ValueError as e:
        print('Failed to parse date: {} - {}'.format(s, e))


def year_to_iri(year):
    return "<http://reference.data.gov.uk/id/year/{}>".format(year)


def convert_account_submissions(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            for row in parse_bcp(bcp_file):
                if all(row):
                    acct_id = '<{}{}/accounts/{}>'.format(PREFIXES['charity'], row[0], row[2])
                    submit_id = '<{}{}/accounts/{}/submission>'.format(PREFIXES['charity'], row[0], row[2])
                    rdf_file.write('{} a ont:accountsSubmission\n'.format(submit_id))
                    rdf_file.write('\t; ont:submissionDate {}'.format(datetime_to_date_iri(row[1])))
                    rdf_file.write('\t; ont:submittedAccounts {}.\n'.format(acct_id))
                    rdf_file.write('{} a ont:financialAccounts\n'.format(acct_id))
                    rdf_file.write('\t; ont:financialYear \n'.format(year_to_iri("20" + row[2][2:])))
                    rdf_file.write('\t.\n')


def convert_aoo(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            for row in parse_bcp(bcp_file):
                id = "area:{}{}".format(row[0], row[1])
                key = row[2]
                desc = row[3]
                if row[0] == 'D':
                    schema_type = "schema:Country"
                else:
                    schema_type = 'schema:AdministrativeArea'
                rdf_file.write("{} a ont:Area, {}\n".format(id, schema_type))
                if key:
                    rdf_file.write('\t; rdfs:label "{}"\n'.format(escape_string(key)))
                    rdf_file.write('\t; schema:name {}\n'.format(escape_string(key)))
                if desc:
                    rdf_file.write('\t; schema:description "{}"'.format(escape_string(desc)))
                rdf_file.write('\t.\n')


def convert_charity_aoo(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            for row in parse_bcp(bcp_file):
                charity = "charity:" + row[0]
                area = "area:{}{}".format(row[1], row[2])
                rdf_file.write("{} ont:areaOfBenefit {}.\n".format(charity, area))


def convert_class(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            for row in parse_bcp(bcp_file):
                charity = "charity:" + row[0]
                cls = "class:" + row[1]
                rdf_file.write('{} ont:charitablePurpose {} .\n'.format(charity, cls))


def convert_class_ref(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            for row in parse_bcp(bcp_file):
                cls = "class:" + row[0]
                label = escape_string(row[1])
                rdf_file.write('{} rdfs:label "{}" .\n'.format(cls, label))


def convert_main_charity(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
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
                    rdf_file.write('\t; ont:companyNumber {}\n'.format(escape_string(company_no.strip())))
                if fyearend_day:
                    rdf_file.write('\t; ont:financialYearEndDayOfMonth {}\n'.format(fyearend_day))
                if fyearend_month:
                    rdf_file.write('\t; ont:financialYearEndMonthOfYear {}\n'.format(fyearend_month))
                if email:
                    to_hash = "mailto:" + email.strip()
                    mbox_sha1sum = hashlib.sha1(to_hash.encode('utf-8')).hexdigest()
                    rdf_file.write('\t; foaf:mbox_sha1sum "{}"\n'.format(escape_string(mbox_sha1sum)))
                if web:
                    rdf_file.write('\t; schema:url "{}"^^schema:URL\n'.format(web))


def convert_name(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            for row in parse_bcp(bcp_file):
                charity = charity_iri(row[0], row[1])
                name = escape_string(row[3])
                rdf_file.write('{0} rdfs:label "{1}"; schema:name "{1}" .\n'.format(charity, name))


def convert_objectives(bcp_path, rdf_path):
    if os.path.exists(rdf_path):
        print('RDF file already exists at {}. Skipping.'.format(rdf_path))
        return
    with open(bcp_path, 'rb') as bcp_file:
        with open(rdf_path, 'w', encoding='utf-8') as rdf_file:
            write_prefixes(rdf_file)
            o=[]
            current_charity = None
            for row in parse_bcp(bcp_file):
                charity = charity_iri(row[0], row[1])
                if charity != current_charity:
                    if len(o):
                        rdf_file.write('{} ont:objective "{}" .\n'.format(current_charity, escape_string(join_continuation_strings(o))))
                    current_charity = charity
                    o = []
                ix = int(row[2])
                while len(o) < ix+1:
                    o.append('')
                o[ix] = row[3]
            if len(o):
                rdf_file.write('{} ont:objective "{}" .\n'.format(current_charity, escape_string(join_continuation_strings(o))))


def join_continuation_strings(arry):
    if len(arry) == 1:
        return arry[0]
    for i in range(0, len(arry)-1):
        if arry[i][-4:] == '0001':
            arry[i] = arry[i][:-4]
    return ''.join(arry)

if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Convert Charity Commission data dump to CSV or RDF')
    p.add_argument('output', choices=['rdf', 'csv'])
    p.add_argument('source_dir')
    p.add_argument('target_dir')
    opts = p.parse_args()
    if opts.output == 'csv':
        convert_to_csv(opts.source_dir, opts.target_dir)
    else:
        convert_to_rdf(opts.source_dir, opts.target_dir)