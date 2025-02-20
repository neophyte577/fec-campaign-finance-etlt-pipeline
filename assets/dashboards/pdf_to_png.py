from pdf2image import convert_from_path

def convert(title):
    poppler_path = r'C:/Program Files/Poppler/bin' 
    pages = convert_from_path(f'./assets/dashboards/{title}.pdf', poppler_path=poppler_path)

    if len(pages) == 1:
        pages[0].save(f'./assets/dashboards/{title}.png', 'PNG')
    else:
        for page_number, page in enumerate(pages):
            page.save(f'./assets/dashboards/{title}_page_{page_number+1}.png', 'PNG')

def main():

    titles = ['individual_contributions']

    for title in titles:
        convert(title)

if __name__ == '__main__':
    main()
