import time
import requests
import csv

BATCH_SIZE = 20

def check_link(url):
    retries = 2
    for i in range(retries):
        try:
            response = requests.get(url, allow_redirects=False, timeout=5)
            if response.status_code == 200:
                print(f"{url} is working fine.")
                return url, 'https'
            elif response.status_code == 301 or response.status_code == 302:
                print(f"{url} is redirecting to {response.headers['Location']}.")
                return response.headers['Location'], 'https'
            else:
                print(f"{url} is broken.")
                return 'NW', 'https'
        except requests.exceptions.SSLError:
            url = url.replace("https", "http")
            try:
                response = requests.get(url, allow_redirects=False, timeout=10)
                if response.status_code == 200:
                    print(f"{url} is working fine over http.")
                    return url, 'http'
                elif response.status_code == 301 or response.status_code == 302:
                    print(f"{url} is redirecting to {response.headers['Location']} over http.")
                    return response.headers['Location'], 'http'
                else:
                    print(f"{url} is broken.")
                    return 'NW', 'http'
            except requests.exceptions.RequestException as e:
                print(f"Error occurred while checking {url}: {e}")
                return 'NW', 'https'
        except requests.exceptions.ConnectionError:
            if i < retries - 1:
                print(f"Connection error occurred while checking {url}. Retrying in 1 second...")
                time.sleep(1)
            else:
                print(f"Max retries exceeded while checking {url}.")
                return 'NW', 'https'
        except requests.exceptions.Timeout:
            print(f"Timeout occurred while checking {url}.")
            return 'NW', 'https'

try:
    with open('websites.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        rows = []
        counter = 0
        for row in csv_reader:
            url = row['Website']
            if not url.startswith('http'):
                url = f"https://{url}"
            new_url, status = check_link(url)
            row['Website'] = new_url
            row['Status'] = status
            rows.append({k: v for k, v in row.items() if k in ['Company Name', 'Website', 'Status']})
            counter += 1
            if counter % BATCH_SIZE == 0:
                with open('updated_websites.csv', mode='a', newline='') as file:
                    fieldnames = ['Company Name', 'Website', 'Status']
                    csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
                    if counter == BATCH_SIZE:
                        csv_writer.writeheader()
                    for r in rows:
                        csv_writer.writerow(r)
                rows = []
        # write any remaining rows to the CSV file
        if rows:
            with open('updated_websites.csv', mode='a', newline='') as file:
                fieldnames = ['Company Name', 'Website', 'Status']
                csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
                for r in rows:
                    csv_writer.writerow(r)
        
    print("Done!")
except Exception as e:
    print(f"An error occurred: {e}")
