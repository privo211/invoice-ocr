"""Offline coverage for Seminis certificate detection and tabular OCR values."""

from io import BytesIO
from html.parser import HTMLParser

import fitz
import pytest
import requests

from test_lot_creation_fields import app_module, _authenticated_client
from vendor_extractors import seminis


def _line(content, x, y, width=70, height=10):
    return {'content': content, 'polygon': [x, y, x + width, y, x + width, y + height, x, y + height]}


def _report(lot='105466356', pure='99.8', inert='0.1', germ='96', date='09/17/2026'):
    # Azure may list all column headings before their values, or read a
    # neighboring table first. The date in the moisture table is a decoy.
    lines = [
        _line('R E P O R T  O F  S E E D  A N A L Y S I S', 100, 40, 400),
        _line(f'Lot Number: {lot}', 300, 200, 180),
        _line('PURITY ANALYSIS', 50, 270),
        _line(f'Pure Seed % {pure}', 320, 280, 120),
        _line(f'Inert Matter % {inert}', 320, 300, 120),
        _line('GERMINATION', 50, 440, 90),
        _line('Date Tested', 150, 440, 55),
        _line('Test Duration (Days)', 220, 440, 65),
        _line('Number of', 295, 440, 45),
        _line('Germination %', 350, 440, 65),
        _line('Hard %', 425, 440, 35),
        _line('Total Viability %', 470, 440, 80),
        _line('MOISTURE CONTENT', 50, 500, 120),
        _line('Date Tested', 180, 500, 70),
        _line('01/01/2026', 180, 520, 70),
        _line('260000923071', 50, 480, 90),
        _line(date, 150, 460, 55),
        _line('7', 245, 475, 10),
        _line('400', 310, 475, 20),
        _line(germ, 365, 460, 20),
        _line('0', 435, 460, 10),
        _line('98', 500, 465, 20),
    ]
    return {'pageNumber': 1, 'lines': lines}


def _text_pdf(lines):
    with fitz.open() as doc:
        page = doc.new_page()
        page.insert_text((40, 40), '\n'.join(lines), fontsize=8)
        return doc.tobytes()


def _scan_pdf(stamped=True):
    with fitz.open(stream=_text_pdf(['Scanned certificate']), filetype='pdf') as source:
        image = source[0].get_pixmap().tobytes('png')
    with fitz.open() as doc:
        page = doc.new_page()
        page.insert_image(page.rect, stream=image)
        if stamped:
            page.insert_text((300, 300), 'Seed Shipment Release From Customs\n'
                             'Purity and/or germination certificate approved', fontsize=8)
        return doc.tobytes()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('Certificate tests must not make network requests')
    monkeypatch.setattr(requests.sessions.Session, 'request', blocked)


def _mock_azure(monkeypatch, reports):
    calls = []

    class Response:
        status_code = 202
        headers = {'Operation-Location': 'https://ocr.test/result'}

        def json(self):
            return {'status': 'succeeded', 'analyzeResult': {'pages': [reports[len(calls) - 1]]}}

    monkeypatch.setattr(seminis.requests, 'post', lambda *args, **kwargs: calls.append(kwargs['data']) or Response())
    monkeypatch.setattr(seminis.requests, 'get', lambda *args, **kwargs: Response())
    monkeypatch.setattr(seminis.time, 'sleep', lambda _: None)
    return calls


@pytest.mark.parametrize(('lot', 'pure', 'inert', 'germ', 'date', 'expected_pure', 'expected_inert'), [
    ('105466356', '99.8', '0.1', '96', '09/17/2026', 99.8, 0.1),
    ('105299869', '100.0', 'TR', '99', '08/21/2026', 99.99, 0.01),
    ('105061809', '100.0', 'TR', '97', '12/29/2025', 99.99, 0.01),
    ('105526418', '100.0', 'TR', '97', '09/11/2026', 99.99, 0.01),
    ('4513632829/0310', '99.9', '0', '0', '7/9/2026', 99.9, 0.0),
])
def test_scanned_certificate_reads_values_beneath_headings(
    monkeypatch, lot, pure, inert, germ, date, expected_pure, expected_inert,
):
    calls = _mock_azure(monkeypatch, [_report(lot, pure, inert, germ, date)])
    data = seminis._extract_seminis_analysis_data([('certificate.pdf', _scan_pdf())])

    assert len(calls) == 1
    assert data[lot.split('/')[0]] == {
        'PureSeed': expected_pure, 'InertMatter': expected_inert,
        'Germ': int(germ), 'GermDate': '07/09/2026' if date == '7/9/2026' else date,
    }


def test_native_certificate_keeps_inline_format_and_case_insensitive_labels():
    pdf = _text_pdf([
        'Report of Seed Analysis', 'lot number: 4513632829/0310',
        'pure seed % 99.8', 'inert matter % 0.1',
        'date tested 07/09/2026', 'germination % 96',
    ])
    assert seminis._extract_seminis_analysis_data([('native.pdf', pdf)])['4513632829'] == {
        'PureSeed': 99.8, 'InertMatter': 0.1, 'Germ': 96, 'GermDate': '07/09/2026',
    }


def test_native_certificate_with_customs_stamp_does_not_require_ocr():
    pdf = _text_pdf([
        'Report of Seed Analysis', 'Lot Number: 105466356',
        'Pure Seed % 99.8', 'Inert Matter % 0.1',
        'Germination % 96', 'Date Tested 09/17/2026',
        'Seed Shipment Release From Customs',
    ])
    assert seminis.extract_text_with_fallback(pdf)['method'] == 'PyMuPDF'


def test_unstamped_scan_requires_ocr(monkeypatch):
    calls = _mock_azure(monkeypatch, [_report()])
    assert seminis.extract_text_with_fallback(_scan_pdf(stamped=False))['method'] == 'Azure OCR'
    assert len(calls) == 1


def test_native_cover_does_not_hide_a_scanned_certificate_page(monkeypatch):
    with fitz.open(stream=_text_pdf(['Seminis certificate cover']), filetype='pdf') as doc:
        with fitz.open(stream=_scan_pdf(stamped=False), filetype='pdf') as scan:
            doc.insert_pdf(scan)
        pdf = doc.tobytes()
    calls = _mock_azure(monkeypatch, [_report()])
    assert seminis._extract_seminis_analysis_data([('mixed.pdf', pdf)])['105466356']['Germ'] == 96
    assert len(calls) == 1


def test_native_certificate_preserves_column_positions():
    report = _report()
    with fitz.open() as doc:
        page = doc.new_page()
        for line in report['lines']:
            x, y = line['polygon'][:2]
            page.insert_text((x, y), line['content'], fontsize=7)
        pdf = doc.tobytes()
    data = seminis._extract_seminis_analysis_data([('columns.pdf', pdf)])
    assert data['105466356']['Germ'] == 96
    assert data['105466356']['GermDate'] == '09/17/2026'


@pytest.mark.parametrize('missing', ['germ', 'date', 'both'])
def test_missing_result_does_not_use_neighboring_columns_or_moisture_date(monkeypatch, missing):
    page = _report()
    remove = {'96'} if missing == 'germ' else {'09/17/2026'}
    if missing == 'both':
        remove = {'96', '09/17/2026'}
    page['lines'] = [line for line in page['lines'] if line['content'] not in remove]
    _mock_azure(monkeypatch, [page])
    data = seminis._extract_seminis_analysis_data([('missing.pdf', _scan_pdf())])['105466356']
    assert data['Germ'] == (None if missing in {'germ', 'both'} else 96)
    assert data['GermDate'] == (None if missing in {'date', 'both'} else '09/17/2026')


def test_invalid_percentages_and_dates_remain_blank(monkeypatch):
    _mock_azure(monkeypatch, [_report(pure='101', inert='102', germ='103', date='13/32/2026')])
    data = seminis._extract_seminis_analysis_data([('invalid.pdf', _scan_pdf())])['105466356']
    assert all(value is None for value in data.values())


def _batch():
    specs = [
        ('BRISTOL', '105466356', '0262662924', '99.8', '0.1', '96', '09/17/2026'),
        ('CORENTINE', '105299869', '0262384200', '100.0', 'TR', '99', '08/21/2026'),
        ('CORENTINE', '105299869', '0262528510', '100.0', 'TR', '99', '08/21/2026'),
        ('FANCIPAK', '105061809', '0252408384', '100.0', 'TR', '97', '12/29/2025'),
        ('SPEEDWAY', '105526418', '0262394491', '100.0', 'TR', '97', '09/11/2026'),
    ]
    invoice = ['Invoice Number: 916251658', 'PO #: 91278', 'Amount']
    packing = ['Packing List']
    for name, lot, batch, *_ in specs:
        invoice.extend([
            f'HYBRID CUCUMBER - {name}', 'TRT: NOT TREATED', '4 x 15 MK POUCH',
            lot, 'CL', batch, '60 MK', 'Total Item', '12.22', '732.96',
        ])
        packing.extend(['TRT: NOT TREATED', f'37092 / 16825 {lot} 99 94 09/25/2026 {batch}'])
    files = [('invoice.pdf', _text_pdf(invoice)), ('packing.pdf', _text_pdf(packing))]
    files.extend((f'{name}-{batch}.pdf', _scan_pdf()) for name, _, batch, *_ in specs)
    reports = [_report(lot, pure, inert, germ, date) for _, lot, _, pure, inert, germ, date in specs]
    return files, reports


def test_batch_reads_each_scan_once_and_attaches_certificates_by_lot(monkeypatch):
    files, reports = _batch()
    calls = _mock_azure(monkeypatch, reports)
    logs = []
    monkeypatch.setattr(seminis, 'log_processing_event', lambda **kwargs: logs.append(kwargs))
    results = seminis.extract_seminis_data_from_bytes(files, ['15,000 SEEDS'])
    items = results['invoice.pdf']

    assert len(items) == 5
    assert len(calls) == 5
    assert len(logs) == 7
    assert set(results) == {'invoice.pdf'}
    assert [item['Germ'] for item in items] == [96, 99, 99, 97, 97]
    assert [item['GermDate'] for item in items] == [
        '09/17/2026', '08/21/2026', '08/21/2026', '12/29/2025', '09/11/2026',
    ]
    assert [item['Purity'] for item in items] == [99.8, 99.99, 99.99, 99.99, 99.99]
    assert all(item['PackingGerm'] == 94 and item['PackingGermDate'] == '09/25/2026' for item in items)


class _Fields(HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}
        self.current = None

    def handle_starttag(self, tag, attrs):
        if tag == 'div':
            self.current = dict(attrs).get('data-field')
            if self.current:
                self.fields.setdefault(self.current, []).append('')

    def handle_data(self, data):
        if self.current:
            self.fields[self.current][-1] += data.strip()

    def handle_endtag(self, tag):
        if tag == 'div':
            self.current = None


def test_upload_populates_certificate_fields_on_all_five_review_rows(app_module, monkeypatch):
    files, reports = _batch()
    _mock_azure(monkeypatch, reports)
    monkeypatch.setattr(seminis, 'log_processing_event', lambda **kwargs: None)
    monkeypatch.setattr(app_module, 'extract_seminis_data_from_bytes', seminis.extract_seminis_data_from_bytes)
    monkeypatch.setattr(app_module, 'prefill_tracking_numbers', lambda *args: None)
    monkeypatch.setattr(app_module, 'token_is_valid', lambda token: True)
    monkeypatch.setattr(app_module, 'load_package_descriptions', lambda token: ['15,000 SEEDS'])
    monkeypatch.setattr(app_module, 'load_treatments', lambda *args: [])
    monkeypatch.setattr(app_module, 'get_po_items', lambda *args: [])
    client = _authenticated_client(app_module, monkeypatch)
    response = client.post('/', data={
        'vendor': 'seminis', 'pdfs': [(BytesIO(data), name) for name, data in files],
    })
    assert response.status_code == 200
    fields = _Fields()
    fields.feed(response.get_data(as_text=True))
    assert fields.fields['GrowerGerm'] == ['96', '99', '99', '97', '97']
    assert fields.fields['GrowerGermDate'] == [
        '09/17/2026', '08/21/2026', '08/21/2026', '12/29/2025', '09/11/2026',
    ]
    assert fields.fields['Purity'] == ['99.8', '99.99', '99.99', '99.99', '99.99']
    assert fields.fields['Inert'] == ['0.1', '0.01', '0.01', '0.01', '0.01']


def test_certificate_zero_values_are_visible(app_module):
    with app_module.app.test_request_context():
        html = app_module.render_template('results_seminis.html', items={'invoice.pdf': [{
            'VendorItemDescription': 'SAMPLE', 'Germ': 0, 'GermDate': None,
            'Purity': 99.9, 'InertMatter': 0, 'USD_Actual_Cost_$': '1.0000',
        }]}, treatments1=[], treatments2=[], pkg_descs=[])
    fields = _Fields()
    fields.feed(html)
    assert fields.fields['GrowerGerm'] == ['0']
    assert fields.fields['Inert'] == ['0']
