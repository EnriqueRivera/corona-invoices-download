import requests
import re
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

disable_warnings(InsecureRequestWarning)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.facturacioncfdigm.modelo.gmodelo.com.mx/ModeloFacturaPRD/',
    'DNT': '1',
}

def fetch_with_retry(session, url, method='GET', retries=5, delay=2, **kwargs):
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
    """Extract ASP.NET hidden form fields"""
    return {
        '__VIEWSTATE': soup.find('input', {'name': '__VIEWSTATE'})['value'],
        '__EVENTVALIDATION': soup.find('input', {'name': '__EVENTVALIDATION'})['value'],
        '__VIEWSTATEGENERATOR': soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value'],
    }

def process_page(session, base_url, params, target_month, target_year):
    """Process a single page and return records + next page params"""
    time.sleep(1.5)
    
    try:
        response = fetch_with_retry(
            session=session,
            url=base_url,
            method='POST',
            data=params,
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

    if oldest_date and (oldest_date.year < target_year or 
                       (oldest_date.year == target_year and 
                        oldest_date.month < target_month)):
        return records, None

    next_link = soup.find('a', string=re.compile(r'Siguiente|Next'))
    if not next_link:
        return records, None

    match = re.search(r"__doPostBack\('(.*?)','(.*?)'\)", next_link.get('href', ''))
    if not match:
        return records, None

    new_params = {
        '__EVENTTARGET': match.group(1),
        '__EVENTARGUMENT': match.group(2),
        **get_hidden_fields(soup)
    }

    return records, new_params

def main(month, year):
    # Create unique timestamped directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"Invoices_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    base_url = "https://www.facturacioncfdigm.modelo.gmodelo.com.mx/ModeloFacturaPRD/Modulos/ClienteInternet/ConsultaCFDHistorico.aspx"
    params = {
        "rfcRec": "RINS6910023U7",
        "rfcEmi": "AMH080702RMA",
        "idEmi": "9",
        "externo": "0"
    }

    with requests.Session() as session:
        session.headers.update(HEADERS)
        
        try:
            response = fetch_with_retry(
                session=session,
                url=f"{base_url}?{urlencode(params)}",
                method='GET',
                verify=False
            )
        except Exception as e:
            print(f"Initialization failed: {str(e)}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        params.update(get_hidden_fields(soup))

        all_records = []
        page_num = 1
        
        while True:
            print(f"Processing page {page_num}...")
            records, new_params = process_page(session, base_url, params, month, year)
            
            if not records and page_num == 1:
                print("No matching records found on first page")
                break
                
            all_records.extend(records)
            
            if not new_params:
                break
                
            params = new_params
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
                            "rfcRec": params["rfcRec"],
                            "itu": record['itu'],
                            "rfcEmi": params["rfcEmi"]
                        },
                        verify=False
                    )

                    if 'application/pdf' in response.headers.get('Content-Type', ''):
                        filename = os.path.join(output_dir, f"{record['itu']}.pdf")
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        downloaded += 1
                        print(f"Downloaded: {filename}")
                    else:
                        print(f"Unexpected content type for {record['itu']}")
                except Exception as e:
                    print(f"Failed to download {record['itu']}: {str(e)}")
            
            print(f"\nSuccessfully downloaded {downloaded}/{len(all_records)} files")
            print(f"All files saved in: {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    main(3, 2025)