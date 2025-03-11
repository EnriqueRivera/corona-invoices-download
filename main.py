import requests
import re
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from PyPDF2 import PdfMerger

disable_warnings(InsecureRequestWarning)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'DNT': '1',
}

def fetch_with_retry(session, url, method='GET', retries=5, delay=10, **kwargs):
    """Generic retry wrapper for HTTP requests"""
    for attempt in range(retries):
        try:
            response = session.request(method, url, **kwargs)
            if response.status_code == 200:
                return response
            print(f"Attempt {attempt+1}/{retries} failed with status {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}/{retries} failed with error: {str(e)}")
        
        if attempt < retries - 1:
            time.sleep(delay)
    
    raise Exception(f"Request failed after {retries} attempts: {method} {url}")

def get_hidden_fields(soup):
    """Extract all ASP.NET hidden form fields including custom ones"""
    fields = {
        '__VIEWSTATE': soup.find('input', {'name': '__VIEWSTATE'})['value'],
        '__EVENTVALIDATION': soup.find('input', {'name': '__EVENTVALIDATION'})['value'],
        '__VIEWSTATEGENERATOR': soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value'],
        '__VIEWSTATEENCRYPTED': soup.find('input', {'name': '__VIEWSTATEENCRYPTED'})['value'] 
            if soup.find('input', {'name': '__VIEWSTATEENCRYPTED'}) else '',
    }
    
    additional_fields = [
        'hidEmisor', 'hInvocacionExterna', 'rfcRec', 'rfcEmi',
        'idEmi', 'externo', 'itu', 'hidItu'
    ]
    
    for field in additional_fields:
        tag = soup.find('input', {'name': field})
        if tag and tag.get('value'):
            fields[field] = tag['value']
        else:
            fields[field] = ''

    return fields

def process_page(session, base_url, query_params, form_params, target_month, target_year):
    """Process a single page with proper ASP.NET headers and data"""
    time.sleep(2)
    
    try:
        full_url = f"{base_url}?{urlencode(query_params)}"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': full_url,
            'Origin': 'https://www.facturacioncfdigm.modelo.gmodelo.com.mx'
        }
        full_form_params = {**query_params, **form_params}
        
        response = fetch_with_retry(
            session=session,
            url=full_url,
            method='POST',
            data=full_form_params,
            headers=headers,
            verify=False
        )
    except Exception as e:
        print(f"Page processing failed: {str(e)}")
        return [], None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'gdHistorico'})
    
    records = []
    oldest_date = None
    
    if table:
        for row in table.find_all('tr')[1:]:
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) >= 7:
                try:
                    date_part = cells[3].split()[0]
                    date_obj = datetime.strptime(date_part, '%d/%m/%Y')
                    
                    if date_obj.month == target_month and date_obj.year == target_year:
                        records.append({
                            'date': date_obj,
                            'itu': cells[6]
                        })
                    
                    if oldest_date is None or date_obj < oldest_date:
                        oldest_date = date_obj
                        
                except (ValueError, IndexError, KeyError):
                    continue

    stop_pagination = False
    if oldest_date:
        target_date = datetime(target_year, target_month, 1)
        if oldest_date < target_date:
            stop_pagination = True

    next_link = soup.find('a', {'id': 'NextPageLink'})
    new_form_params = None

    if not stop_pagination and next_link:
        href = next_link.get('href', '')
        match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
        
        if match:
            current_hidden = get_hidden_fields(soup)
            new_form_params = {
                **current_hidden,
                '__EVENTTARGET': match.group(1),
                '__EVENTARGUMENT': match.group(2)
            }

    return records, new_form_params

def merge_and_cleanup(output_dir, all_records, timestamp):
    """Merge downloaded PDFs and clean up individual files"""
    if not PdfMerger:
        print("PyPDF2 not installed. Skipping merge and cleanup.")
        return

    merged_filename = os.path.join(output_dir, f"merged_invoices_{timestamp}.pdf")
    merger = PdfMerger()
    deleted_count = 0

    try:
        # Merge files in chronological order
        for record in sorted(all_records, key=lambda x: x['date']):
            pdf_path = os.path.join(output_dir, f"{record['itu']}.pdf")
            if os.path.exists(pdf_path):
                merger.append(pdf_path)
        
        with open(merged_filename, 'wb') as merged_file:
            merger.write(merged_file)
        
        print(f"\nMerged {len(all_records)} invoices into {merged_filename}")

        # Delete individual files
        for record in all_records:
            pdf_path = os.path.join(output_dir, f"{record['itu']}.pdf")
            try:
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                    deleted_count += 1
            except Exception as e:
                print(f"Error deleting {pdf_path}: {str(e)}")
        
        print(f"Deleted {deleted_count} individual PDF files")

    except Exception as e:
        print(f"Error during PDF operations: {str(e)}")
    finally:
        merger.close()

def main(rfc, month, year):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"Invoices_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    base_url = "https://www.facturacioncfdigm.modelo.gmodelo.com.mx/ModeloFacturaPRD/Modulos/ClienteInternet/ConsultaCFDHistorico.aspx"
    query_params = {
        "rfcRec": rfc,
        "rfcEmi": "AMH080702RMA",
        "idEmi": "9",
        "externo": "0"
    }

    with requests.Session() as session:
        session.headers.update(HEADERS)
        
        try:
            initial_url = f"{base_url}?{urlencode(query_params)}"
            response = fetch_with_retry(
                session=session,
                url=initial_url,
                method='GET',
                verify=False
            )
        except Exception as e:
            print(f"Initialization failed: {str(e)}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        form_params = get_hidden_fields(soup)

        all_records = []
        page_num = 1
        
        while True:
            print(f"Processing page {page_num}...")
            records, new_form_params = process_page(
                session, base_url, query_params, form_params, month, year
            )
            
            if not records and page_num == 1:
                print("No matching records found on first page")
                break
                
            all_records.extend(records)
            
            if not new_form_params:
                break
                
            form_params = new_form_params
            page_num += 1

        print(f"\nFound {len(all_records)} matching records")
        
        if all_records:
            downloaded = 0
            for record in all_records:
                try:
                    pdf_url = "https://www.facturacioncfdigm.modelo.gmodelo.com.mx/ModeloFacturaPRD/Modulos/ClienteInternet/VistaCFDpdf.aspx"
                    response = fetch_with_retry(
                        session=session,
                        url=pdf_url,
                        method='GET',
                        params={
                            "rfcRec": query_params["rfcRec"],
                            "itu": record['itu'],
                            "rfcEmi": query_params["rfcEmi"]
                        },
                        verify=False
                    )

                    if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
                        filename = os.path.join(output_dir, f"{record['itu']}.pdf")
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        downloaded += 1
                        print(f"Downloaded: {filename}")
                    else:
                        print(f"Failed to download {record['itu']} - Status: {response.status_code}")
                except Exception as e:
                    print(f"Failed to download {record['itu']}: {str(e)}")
            
            print(f"\nSuccessfully downloaded {downloaded}/{len(all_records)} files")
            print(f"Files saved to: {os.path.abspath(output_dir)}")

            # Merge and cleanup after successful download
            merge_and_cleanup(output_dir, all_records, timestamp)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("rfc", type=str, help="RFC")
    parser.add_argument("month", type=int, help="Month (1-12)")
    parser.add_argument("year", type=int, help="Year (e.g., 2023)")
    args = parser.parse_args()
    
    #RINS6910023U7
    main(args.rfc, args.month, args.year)