import requests
from bs4 import BeautifulSoup

URL = "https://docs.google.com/document/d/e/2PACX-1vSvM5gDlNvt7npYHhp_XfsJvuntUhq184By5xO_pA4b_gCWeXb6dM6ZxwN8rE6S4ghUsCj2VKR21oEP/pub"


def print_secret_message(doc_url):
    response = requests.get(doc_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    rows = soup.find('table').find_all('tr')[1:]  # skip header row

    points = set()
    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 3:
            continue
        x = int(cells[0].get_text(strip=True))
        y = int(cells[2].get_text(strip=True))
        points.add((x, y))

    max_x = max(x for x, _ in points)
    max_y = max(y for _, y in points)

    def render(on, off):
        # y=0 is the bottom row, so print from max_y down to 0
        return '\n'.join(
            ''.join(on if (x, y) in points else off for x in range(max_x + 1))
            for y in range(max_y, -1, -1)
        )

    # 1 char per cell: 90 columns wide, fits in a normal terminal
    print(render('#', ' '))

    # 2 chars per cell: better proportions, view in an editor with word wrap OFF
    with open('out.txt', 'w', encoding='utf-8') as f:
        f.write(render('##', '  '))


print_secret_message(URL)