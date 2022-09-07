import geocoder
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import pymysql
import numpy as np

def login(lat, lng, city, s):
    url = f'https://blinkit.com/mapAPI/autosuggest_google?lat={lat}&lng={lng}&query={city}'
    res = requests.get(url)
    reqUrl = "https://blinkit.com/"
    headersList = {
        "Accept": "*/*",
        "User-Agent": "Thunder Client (https://www.thunderclient.com)",
        "authority": "blinkit.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
    }
    payload = ""
    response = requests.request("GET", reqUrl, data=payload, headers=headersList)
    # print(response.text)
    r = re.search(r'"requestKey":"[0-9a-zA-Z-]{10,40}"', response.text).group()
    bot = re.search(r'"mockAuthKey":"[0-9a-zA-Z-]{50,70}"', response.text).group()
    deviceId = re.search(r'"deviceId":"[0-9a-zA-Z-]{20,36}"', response.text).group()
    deviceId = r.split('"')[3]
    soup = BeautifulSoup(response.text, 'html.parser')
    dd = (soup.find_all("script")[12])
    auth_key = bot.split('"')[3]
    req_key = r.split('"')[3]
    s.cookies.set('gr_1_deviceId', f'{deviceId}')
    print(req_key)


def get_products(s,query):
    url2 = f'https://blinkit.com/mapAPI/autosuggest_google?lat={lat}&lng={lng}&query={pincode}'
    print(url2)
    respp = s.get(url2)
    url3 = f'https://blinkit.com/v5/search/merchants/31414/products/?lat={lat}&lon={lng}&q={query}&suggestion_type=0&t=1&start=0&size=100'
    resp = s.get(url3)
    count = 0
    data = []
    while True:
        # print(count)
        try:
            for i in resp.json():
                count += 1
                # print(resp.json()['widget_response']['objects'][count])
                data.append(resp.json()['widget_response']['objects'][count])
                break
        except IndexError as e:
            print(e)
            break
    print("Aprajita singh")

    return data

def iter_data(data):
    # print(data)
    Blinkit_data = []
    for i in data:
        # dd =i['objects'][1]['data']#['product']
        for j in i['objects']:
            # print(j)
            try:
                channel= 'Blinkit'
                v_price =j['data']['product']['price']
                v_mrp = j['data']['product']['mrp']
                v_unit = j['data']['product']['unit']
                offer =j['data']['product']['offer']
                v_disc_amt = j['data']['product']['disc_amt']
                brand = j['data']['product']['attributes']['brand']
                name = j['data']['product']['name']
                product_id = j['data']['product']['product_id']
                inventory = j['data']['product']['inventory']
                try:
                    keywords = j['data']['product']['attributes']['key_features']
                    description = j['data']['product']['attributes']['description']
                except KeyError:
                    keywords= None
                    description= None
                available = j['data']['product']['availability']['is_available']
                available_from = j['data']['product']['availability']['available_from']
                images = len(j['data']['product']['images'])
                main_image = j['data']['product']['images'][0]
                try:
                    image1 = j['data']['product']['images'][0]
                    image2 =  j['data']['product']['images'][1]
                    image3 = j['data']['product']['images'][2]
                    image4 = j['data']['product']['images'][3]
                    image5 = j['data']['product']['images'][4]
                    image6 = j['data']['product']['images'][5]
                    image7 = j['data']['product']['images'][6]
                    image8 = j['data']['product']['images'][7]
                    image9 = j['data']['product']['images'][8]
                    image10 = j['data']['product']['images'][9]
                except Exception as e:
                    print(e)
                # print(brand,product_id,str(name+v_unit),v_price,v_mrp,v_unit,offer,v_disc_amt,inventory,keywords,description,
                #       available,available_from,images)
                if str(brand).lower().__contains__(f'{str(query).lower()}'):

                    Blinkit_data.append((channel,brand,product_id,str(name+' '+ v_unit),v_price,v_mrp,v_unit,offer,v_disc_amt,inventory,keywords,description,
                                         available,available_from,images,main_image, image1, image2, image3, image4, image5, image6, image7,
                                         image8,image9,image10))

                df = pd.DataFrame(Blinkit_data, columns=["channel","brand","product_id","name","v_price","v_mrp","v_unit","offer",
                                                     "v_disc_amt","inventory","keywords","description","available","available_from",
                                                    "images","main_image","image1","image2","image3","image4",
                                                    "image5","image6","image7","image8","image9","image10"])

            except Exception as e:
                    print(e)
    print(df.to_csv('newP.csv', index=False))

    return df

def post_data(df):
    df  = df.replace({np.nan:None})
    for ind,i in df.iterrows():
        print(i)
        channel = (i[0])
        brand = (i[1])
        product_id = (i[2])
        name = (i[3])
        v_price = (i[4])
        v_mrp = (i[5])
        v_unit = (i[6])
        offer = (i[7])
        v_disc_amt = (i[8])
        inventory = (i[9])
        keywords = (i[10])
        description = (i[11])
        available = (i[12])
        available_from = (i[13])
        images = (i[14])
        main_image = (i[15])
        image1 = (i[16])
        image2 = (i[17])
        image3 = (i[18])
        image4 = (i[19])
        image5 = (i[20])
        image6 = (i[21])
        image7 = (i[22])
        image8 = (i[23])
        image9 = (i[24])
        image10 = (i[25])
     # Table create query::::
        con = pymysql.connect(host='localhost',
                              user='root',
                              database='mydb')
        cur = con.cursor()
            # query = """
        #          CREATE TABLE IF NOT EXISTS finalblinkit_data
        #          (
        #
        #                             channel VARCHAR(255) NOT NULL,
        #                             brand VARCHAR(255) NOT NULL,
        #                             product_id VARCHAR(255)NOT NULL,
        #                             name VARCHAR(255)NOT NULL,
        #                             v_price VARCHAR(255)NOT NULL,
        #                             v_mrp VARCHAR(255) NOT NULL,
        #                             v_unit VARCHAR(255) NOT NULL,
        #                             offer VARCHAR(255) NOT NULL,
        #                             v_disc_amt VARCHAR(255) NOT NULL,
        #                             inventory VARCHAR(255) NOT NULL,
        #                             keywords VARCHAR(255) NOT NULL,
        #                             description VARCHAR(255) NOT NULL,
        #                             available VARCHAR(255) NOT NULL,
        #                             available_from VARCHAR(255) NOT NULL,
        #                             images VARCHAR(255) NOT NULL,
        #                             main_image VARCHAR(255) NOT NULL,
        #                             image1 VARCHAR(255) NOT NULL,
        #                             image2 VARCHAR(255) NOT NULL,
        #                             image3 VARCHAR(255) NOT NULL,
        #                             image4 VARCHAR(255) NOT NULL,
        #                             image5 VARCHAR(255) NOT NULL,
        #                             image6 VARCHAR(255) NOT NULL,
        #                             image7 VARCHAR(255) NOT NULL,
        #                             image8 VARCHAR(255) NOT NULL,
        #                             image9 VARCHAR(255) NOT NULL,
        #                             image10 VARCHAR(255) NOT NULL
        #                             );
        #
        #         """
                # cur.execute(query)
                # con.commit()
                # print(f'{cur.rowcount}Created')
                # print(f'{cur.rowcount} row dupdated/inserte')
        query = """
                                        INSERT INTO finalblinkit_data
                                                (channel, brand, product_id, name, v_price,  v_mrp, v_unit, offer, v_disc_amt,
                                                inventory, keywords, description, available, available_from, images, main_image,image1, image2, image3, image4,
                                                image5, image6, image7, image8, image9, image10)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        
                                        ON DUPLICATE KEY UPDATE
                                                channel=%s, brand=%s, product_id=%s, name=%s, v_price=%s, v_mrp=%s, v_unit=%s, offer=%s, v_disc_amt=%s, inventory=%s, keywords=%s, 
                                                description=%s, available=%s, available_from=%s, images=%s, main_image=%s, image1=%s, image2=%s, image3=%s,
                                                image4=%s, image5=%s, image6=%s, image7=%s, image8=%s, image9=%s, image10=%s;
        
                            """
        val = (
                    channel, brand, product_id, name, v_price, v_mrp, v_unit, offer, v_disc_amt, inventory, keywords, description, available,
                    available_from, images, main_image, image1, image2, image3, image4, image5, image6, image7, image8, image9,image10,
                    channel, brand, product_id, name, v_price, v_mrp, v_unit, offer, v_disc_amt, inventory, keywords, description,
                    available,
                    available_from, images, main_image, image1, image2, image3, image4, image5, image6, image7, image8, image9,
                    image10
            )
        # print(val)
        cur.execute(query, val)
        con.commit()
        print("added")
        cur.close()
        con.close()



def main():
    with requests.session() as s:
        s.headers["User-Agent"] = "Thunder Client (https://www.thunderclient.com)"
        s.headers['path'] = f'/mapAPI/autosuggest_google?lat={lat}&lng={lng}&query={pincode}'
        s.headers['authority'] = 'blinkit.com'
        s.headers['accept'] = '*/*'
        s.headers['authority'] = "blinkit.com"
        s.cookies.set('gr_1_deviceId', "d4464987-7baa-40ae-99ab-7df685fa2542")
        s.cookies.set('gr_1_lat', f'{lat}')
        s.cookies.set('gr_1_lon', f'{lng}')
        s.cookies.set('gr_1_landmark', f'{city}')
        s.headers['pragma'] = 'no-cache'
        s.headers['accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        login(lat, lng, city, s)
        data = get_products(s,query)
        df = iter_data(data)
        post_data(df)

if __name__ == '__main__':
    query = "Maggi"
    pincode = 560064
    g = geocoder.google(f"{pincode}", key='AIzaSyBNRtgrXGd_1UfEoXxh3FYREZTxeupYI0k')
    results = g.json
    # print(results) #find all address values
    lat = results['lat']
    lng = results['lng']
    city = results['city']
    main()
