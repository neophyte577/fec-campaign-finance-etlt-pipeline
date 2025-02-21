from pdf2image import convert_from_path
from PIL import Image

def convert_and_merge(title):
    poppler_path = r'C:/Program Files/Poppler/bin'
    
    pages = convert_from_path(f'./assets/dashboards/{title}.pdf', poppler_path=poppler_path)

    if len(pages) == 1:
        pages[0].save(f'./assets/dashboards/{title}.png', 'PNG')
    else:
        width = max(page.width for page in pages)  
        height = sum(page.height for page in pages)  

        merged_image = Image.new('RGB', (width, height), color=(255, 255, 255))

        y_offset = 0
        for page in pages:
            merged_image.paste(page, (0, y_offset))
            y_offset += page.height  
        
        merged_image.save(f'./assets/dashboards/{title}.png', 'PNG')

def main():
    titles = ['individual_contributions', 'operating_expenditures']
    
    for title in titles:
        convert_and_merge(title)

if __name__ == '__main__':
    main()
