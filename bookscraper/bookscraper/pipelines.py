# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        
        adapter = ItemAdapter(item)

        ## strip all leading and trailing whitespace from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value.strip()
        
        ## switch category and product types to all lowercase
        lowercase_keys = ['category','product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        ## strip ascii symbol from prices
        price_keys = ['price_excl_tax','price_incl_tax','tax','price']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '0')
            adapter[price_key] = float(value)

        ## extract number of books in stock from availability
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        ## convert stars to number
        stars_string = adapter.get('stars')
        if stars_string == "Zero":
            adapter['stars'] = 0
        elif stars_string == "One":
            adapter['stars'] = 1
        elif stars_string == "Two":
            adapter['stars'] = 2
        elif stars_string == "Three":
            adapter['stars'] = 3
        elif stars_string == "Four":
            adapter['stars'] = 4
        elif stars_string == "Five":
            adapter['stars'] = 5

        
        return item
    

import mysql.connector

class SaveToMySQLPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = '',
            database = 'books'
        )

        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()

        ## Create books table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment,
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):

        ## Define insert statement
        self.cur.execute(""" insert into books (
            url,
            title,
            upc,
            product_type,
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            stars,
            category,
            description
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s     
                )""", (
            item["url"],
            item["title"],
            item["upc"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["price"],
            item["availability"],
            item["num_reviews"],
            item["stars"],
            item["category"],
            str(item["description"][0])    # i may need [0] after "] inside )"    
        ))

        ## Execute insert of data into database
        self.conn.commit()
        return item
    

    def close_spider(self, spider):

        # Close cursor and connection to db
        self.cur.close()
        self.conn.close()
