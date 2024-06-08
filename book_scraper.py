import json
import os
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from forex_python.converter import CurrencyCodes


def get_currency_symbol(currency_code):
    currency = CurrencyCodes()
    symbol = currency.get_symbol(currency_code)
    if symbol:
        return symbol
    return ""


def remove_extra_space(text):
    while re.search('(^[,;:\s]+|[,;:\s]+\s*$)', text) is not None:
        text = re.sub('(^[,;:\s]+|[,;:\s]+\s*$)', '', text)
    return text


def status_log(r):
    """Pass response as a parameter to this function"""
    url_log_file = 'url_log.txt'
    if not os.path.exists(os.getcwd() + '\\' + url_log_file):
        with open(url_log_file, 'w') as f:
            f.write('url, status_code\n')
    with open(url_log_file, 'a') as file:
        file.write(f'{r.url}, {r.status_code}\n')


def retry(func, retries=10):
    """Decorator function"""
    retry.count = 0

    def retry_wrapper(*args, **kwargs):
        attempt = 0
        while attempt < retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempt += 1
                total_time = attempt * 10
                print(f'Retrying {attempt}: Sleeping for {total_time} seconds, error: ', e)
                time.sleep(total_time)
            if attempt == retries:
                retry.count += 1
                url_log_file = 'url_log.txt'
                if not os.path.exists(os.getcwd() + '\\' + url_log_file):
                    with open(url_log_file, 'w') as f:
                        f.write('url, status_code\n')
                with open(url_log_file, 'a') as file:
                    file.write(f'{args[0]}, requests.exceptions.ConnectionError\n')
            # if retry.count == 3:
            #     print("Stopped after retries, check network connection")
            #     raise SystemExit

    return retry_wrapper


@retry
def get_soup(url, headers=None):
    """returns the soup of the page when given with the of an url and headers"""

    r = requests.get(url, headers=headers)
    r.encoding = r
    if r.status_code == 200:
        return BeautifulSoup(r.text, 'html.parser')
    elif 499 >= r.status_code >= 400:
        print(f'client error response, status code {r.status_code} \nrefer: {r.url}')
        status_log(r)
    elif 599 >= r.status_code >= 500:
        print(f'server error response, status code {r.status_code} \nrefer: {r.url}')
        count = 1
        while count != 1:
            print('while', count)
            r = requests.get(url, headers=headers)
            r.encoding = r
            print('status_code: ', r.status_code)
            if r.status_code == 200:
                return BeautifulSoup(r.text, 'html.parser')
            else:
                print('retry ', count)
                count += 1
                time.sleep(count * 3)
        return None
    else:
        status_log(r)
        return None


def strip_it(text):
    return re.sub('\s+', ' ', text).strip()


def write_data_to_csv(book_number, data_dict=None):
    if data_dict is None:
        data_dict = {
            "ISBN-10": book_number,
            "Title of the Book": "Book Not Found",
            "Author/s": "",
            "Book type": "",
            "Original Price (RRP)": "",
            "Discounted price": "",
            "Published Date": "",
            "Publisher": "",
            "No. of Pages": "",
            "Product URL": ""
        }
    df = pd.DataFrame([data_dict])

    if os.path.isfile(f'{file_name}.csv'):
        df.to_csv(f'{file_name}.csv', index=False, header=False, mode='a')
    else:
        df.to_csv(f'{file_name}.csv', index=False)



if __name__ == '__main__':
    file_name = os.path.basename(__file__).replace('.py', '')
    input_df = pd.read_csv('input_list.csv')
    book_numbers = input_df['ISBN13'].values
    header = {
        'authority': 'www.booktopia.com.au',
        'method': 'GET',
        'path': '/search?keywords=9780007461240&productType=917504&pn=1',
        'scheme': 'https',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        # 'Cookie': '_gid=GA1.3.1101602184.1717765594; gaUniqueIdentifier=D4B200A5-A40B-E91E-457B-32043CA1018D; _pxvid=cdb75421-24ce-11ef-a697-e828fdbca8a7; _gcl_au=1.1.1169724672.1717765614; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%7D; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22kRcfu3nlm7WklI82Bo3U%22%7D; scarab.visitor=%2265A305E508C0FA4B%22; FPID=FPID2.3.LTBXshw2O8kcCvNaep45arRLIfRIynTf9WoQ0wB%2FP40%3D.1717765594; FPLC=fY1OaD410rCNu0yBahxloMpnVqTcqX3bgthBkQTSPZ5CDmbDfC43V9EGPQ7cq9L5sT2yOmguUX6AWJ8Ty%2BEidQdUftcd0iorMQQZIg7RzrpAmdDVzI5DRecqqxTbrA%3D%3D; ftr_ncd=6; _fbp=fb.2.1717765618392.529003652915396633; __attentive_id=0c13498f73794e9c8c51a3b5bea39f5f; _attn_=eyJ1Ijoie1wiY29cIjoxNzE3NzY1NjE4NDU5LFwidW9cIjoxNzE3NzY1NjE4NDU5LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcIjBjMTM0OThmNzM3OTRlOWM4YzUxYTNiNWJlYTM5ZjVmXCJ9In0=; __attentive_cco=1717765618463; __attentive_dv=1; __attentive_ss_referrer=https://www.google.com/; isBotUserAgent=false; scarab.profile=%229780007461240%7C1717765665%22; JSESSIONID=8ZdWLvINrSasbXB0Us_k7FUAW3uf0Bs46uAzxdrc.10.0.103.85; _ttp=S16h0vrR2Y3PIu445JrlV7TWwQl; siteVersion=PC; domainCustomerSessionGuid=F1AE4599-F5EB-4B85-B2AA-F95EBEA06071; _uetsid=d02cfa3024ce11ef9ff9d570297c3871; _uetvid=d02d34c024ce11efb334d50272ee4393; _px3=708aed946c6973a212737ceb4e70dc62c2854c20c73dbfd0488876cc09ce2580:EhPudN5eXTE4u/AO6bO5KNyuW6D0fb94ARx6Z6KWR9J+vGoFby1sjyt+Hd+RBQjAuykQ5OLdYlCaxz4XtRyqWQ==:1000:gRfR/h7PawN3kmm5aH8xIgq2aWIgY6yRPT/kiovhxqmS1dNEJ/+8OQFh+OIbCkacHt5+fcOfefQomF2MlZIgTOJ49TlQbcy1q1mvi3RYiSe6TAax5BYtI6CeNDlqEbhfAwULVk9MzKiFo2n/iCPW83rYM2o0pABBrL+eBVldE33jais9eIIPF7lRCvlY5YEe5cHG1zv7mj2khGUQkLRPjdRUjxOaXP7AgYnBKrXZ+7U=; _ga=GA1.1.1393316323.1717765594; forterToken=b8b70092ceb245fbb9297028fd53bf28_1717766136800__UDF43-m4_6_M9ycSry5FP8%3D-3539-v2; forterToken=b8b70092ceb245fbb9297028fd53bf28_1717766136800__UDF43-m4_6_M9ycSry5FP8%3D-3539-v2; _gat=1; countryCode=AU; __attentive_pv=6; _rdt_uuid=1717765614419.62b366bd-458c-436b-9efd-a20ac8ef3c4b; _ga_XYG4G317GS=GS1.1.1717765614.1.1.1717766227.0.0.903937305; FPGSID=1.1717765612.1717766225.G-XYG4G317GS.MMxR1_tiiNHA7ddSLNKP8A; AWSALB=KaZg23z8/tNC4yvZvOdYGUzV16GiMYuc27qZeSc/IUl5WrI5Kn7d64K0yo0+b05zHwRg7yqNTA6wAMnNdWQBHjP9b9zeKdyWNyQhnIa89C7McBnWbrMsffgkWU2P; AWSALBCORS=KaZg23z8/tNC4yvZvOdYGUzV16GiMYuc27qZeSc/IUl5WrI5Kn7d64K0yo0+b05zHwRg7yqNTA6wAMnNdWQBHjP9b9zeKdyWNyQhnIa89C7McBnWbrMsffgkWU2P',
        'Priority': 'u=0, i',
        'Referer': 'https://www.booktopia.com.au/search?keywords=9780718183226&productType=917504&pn=1',
        'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }
    for book_number in book_numbers:
        print(f"Processing for book number: [{book_number}]")
        book_url = f"https://www.booktopia.com.au/search?keywords={book_number}&productType=917504&pn=1"
        header['path'] = book_url.split('.au', 1)[-1]
        book_soup = get_soup(book_url, headers=header)
        if book_soup is None:  # In case of 404 the soup returns None, and we can know that the book does not exist
            print(f"Book Number not found on website: [{book_number}]")
            write_data_to_csv(book_number=book_number)
            continue

        book_data_json_tag = book_soup.find('script', type='application/ld+json')
        book_data_json = json.loads(book_data_json_tag.text) if book_data_json_tag is not None else []
        if not book_data_json:
            continue
        master_data_dict = book_data_json[0] if type(book_data_json) is list else book_data_json
        offersList = master_data_dict.get("offers", [])
        offerDict = offersList[0]
        price = offerDict.get("price", "")
        priceCurrency = offerDict.get("priceCurrency", "")

        productID = offerDict.get("productID", "")
        url = offerDict.get("url", "")

        authorsListRaw = master_data_dict.get("author", [])
        authorsList = []
        for author_dict in authorsListRaw:
            if author_dict['@type'] != 'Person':
                continue
            authorsList.append(author_dict['name'])

        authors = '; '.join(authorsList)

        # Next set of data available in another json data tag
        detailed_book_json_tag = book_soup.find('script', attrs={'id': '__NEXT_DATA__', 'type': 'application/json'})
        detailed_book_json = json.loads(detailed_book_json_tag.text) if detailed_book_json_tag is not None else {}

        productDict = detailed_book_json.get("props", {}).get("pageProps", {}).get("product", {})

        priceSymbol = get_currency_symbol(priceCurrency)

        bookType = productDict.get("bindingFormat", "")

        retailPrice = productDict.get("retailPrice", "")
        retailPriceWithSymbol = f"{priceSymbol}{retailPrice}"

        salePrice = productDict.get("salePrice", "")
        salePriceWithSymbol = f"{priceSymbol}{salePrice}"

        publicationDate = productDict.get("publicationDate", "")
        publicationDateFormatted = parse(publicationDate).strftime("%Y-%m-%d")

        publisher = productDict.get("publisher", "")
        numberOfPages = productDict.get("numberOfPages", "")

        data_dict = {
            "ISBN-10": book_number,
            "Title of the Book": master_data_dict.get("name", ""),
            "Author/s": authors,
            "Book type": bookType,
            "Original Price (RRP)": retailPriceWithSymbol,
            "Discounted price": salePriceWithSymbol,
            "Published Date": publicationDateFormatted,
            "Publisher": publisher,
            "No. of Pages": numberOfPages,
            "Product URL": url
        }
        write_data_to_csv(book_number=book_number, data_dict=data_dict)
