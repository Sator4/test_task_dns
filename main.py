import psycopg2
import time

cwd = 'C:\STORAGE\Work\Test_task'   # директория, в которой лежит скрипт и файлы таблиц

def fill_tables(connection):    # заполнение таблиц из csv
    cursor = connection.cursor()

    cursor.execute(f"COPY cities FROM '{cwd}\\t_cities.csv' CSV HEADER")
    cursor.execute(f"COPY branches FROM '{cwd}\\t_branches.csv' CSV HEADER")
    cursor.execute(f"COPY products FROM '{cwd}\\t_products.csv' CSV HEADER")
    cursor.execute(f"COPY sales FROM '{cwd}\\t_sales.csv' CSV HEADER")

    connection.commit()
    print('Tables filled.')


def whipe_table(connection, name):  # очистка таблиц
    if name not in ['cities', 'branches', 'products', 'sales', 'all']:
        print('Wrong table name')
        return
    if name == 'all':
        whipe_table(connection, 'sales')
        whipe_table(connection, 'products')
        whipe_table(connection, 'branches')
        whipe_table(connection, 'cities')
        return
    name = 'public.' + name
    cursor = connection.cursor()
    cursor.execute('DELETE FROM ' + name)
    print('Whipe done')

def create_database(connection):  # создание таблиц
    cursor = connection.cursor()
    create_cities = '''
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER,
            link VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )'''
    create_branches = '''
        CREATE TABLE IF NOT EXISTS branches (
            id INTEGER,
            link VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            city_link VARCHAR(50) NOT NULL,
            short_name VARCHAR(50),
            region VARCHAR(50) NOT NULL,

            FOREIGN KEY (city_link) REFERENCES cities(link)
        )'''
    create_products = '''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER,
            link VARCHAR(50) PRIMARY KEY,
            name VARCHAR(200) NOT NULL
        )'''
    create_sales = '''
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            branch_link VARCHAR(50) NOT NULL,
            product_link VARCHAR(50) NOT NULL,
            amount FLOAT NOT NULL,
            revenue FLOAT NOT NULL,
            FOREIGN KEY (branch_link) REFERENCES branches(link),
            FOREIGN KEY (product_link) REFERENCES products(link)
        )'''
    
    create_index_1 = '''
        CREATE INDEX sales_products
	        ON sales(product_link);
        '''
    create_index_2 = '''
        CREATE INDEX sales_time
	        ON sales(timestamp);
        '''
    create_index_3 = '''
        CREATE INDEX products_link
	        ON products(link);
        '''

    cursor.execute(create_cities)
    cursor.execute(create_branches)
    cursor.execute(create_products)
    cursor.execute(create_sales)
    cursor.execute(create_index_1)      # я не знаю, имеют ли какой-то эффект эти индексы на производительность, я лично не заметил
    cursor.execute(create_index_2)
    cursor.execute(create_index_3)
    connection.commit()
    print('Database created')


def drop_database(connection):
    cursor = connection.cursor()
    cursor.execute('DROP TABLE sales')
    cursor.execute('DROP TABLE products')
    cursor.execute('DROP TABLE branches')
    cursor.execute('DROP TABLE cities')
    connection.commit()
    print('Database dropped')


def task_1_1(connection):   # 1.1.	десять самых продаваемых товаров для каждого магазина
    cursor = connection.cursor()
    cursor.execute('''
        ALTER TABLE branches
            ADD IF NOT EXISTS top_10_products VARCHAR(50)[10];
    ''')
    cursor.execute('''
        UPDATE branches SET
        top_10_products = product_array
        FROM (
            SELECT DISTINCT branch_link,
                ARRAY_AGG(product_link) OVER(PARTITION BY branch_link) as product_array
                FROM (
                    SELECT * FROM (
                        SELECT *,
                            RANK() OVER(PARTITION BY branch_link ORDER BY amount_sum DESC, product_link) as amount_rank
                            FROM (
                                SELECT
                                DISTINCT branch_link, product_link,
                                SUM(amount) OVER(PARTITION BY branch_link, product_link) AS amount_sum
                                FROM sales
                            )
                        )
                        WHERE amount_rank <= 10
                )
        ) arr_list
        WHERE branches.link = arr_list.branch_link;
    ''')

    cursor.execute('SELECT * FROM branches')
    for i in cursor.fetchall():
        print(i)


def task_1_2(connection):   # 1.2.	десять самых продаваемых товаров для каждого города
    cursor = connection.cursor()
    cursor.execute('''
        ALTER TABLE cities
            ADD IF NOT EXISTS top_10_products VARCHAR(50)[10];
    ''')
    cursor.execute('''
        UPDATE cities SET
            top_10_products = product_array
            FROM (
                SELECT DISTINCT city_link,
                    ARRAY_AGG(product_link) OVER(PARTITION BY city_link) as product_array
                    FROM (
                        SELECT * FROM (
                            SELECT *,
                                RANK() OVER(PARTITION BY city_link ORDER BY amount_sum DESC, product_link) as amount_rank
                                FROM (
                                    SELECT 
                                        DISTINCT sales.product_link, branches.city_link,
                                        SUM(amount) OVER(PARTITION BY city_link, product_link) as amount_sum
                                        FROM sales
                                        JOIN branches ON sales.branch_link = branches.link
                                )
                            )
                            WHERE amount_rank <= 10
                    )
            ) arr_list
            WHERE cities.link = arr_list.city_link;
    ''')

    cursor.execute('SELECT * FROM cities')
    for i in cursor.fetchall():
        print(i)


def task_1_3(connection):   # 1.3.	десять первых магазинов по количеству проданного товара с суммой продаж
    cursor = connection.cursor()
    cursor.execute('''
        SELECT branches.name, top_10_shops.amount_sum, top_10_shops.revenue_sum
            FROM (
                SELECT * FROM (
                    SELECT 
                        DISTINCT branch_link,
                        SUM(amount) OVER(PARTITION BY branch_link) AS amount_sum,
                        SUM(revenue) OVER(PARTITION BY branch_link) AS revenue_sum
                        FROM sales	
                    ) 
                    ORDER BY amount_sum DESC
                    LIMIT 10
            ) top_10_shops
            JOIN branches ON top_10_shops.branch_link = branches.link
            ORDER BY amount_sum DESC;
    ''')

    for i in cursor.fetchall():
        print(i)


def task_1_4(connection):   # 1.4.	рейтинг товаров согласно суммарному количеству проданного товара за всю историю наблюдений со средним количеством продаж за день по убыванию
    cursor = connection.cursor()
    cursor.execute('''
        WITH datediff AS (
            SELECT DATE_PART('day',
                (SELECT timestamp FROM sales ORDER BY timestamp DESC LIMIT 1)::timestamp - 
                (SELECT timestamp FROM sales ORDER BY timestamp ASC LIMIT 1)::timestamp
            ) date_part
        )
        SELECT products.name, top_products.average_per_day
            FROM (
                SELECT
                    DISTINCT product_link,
                    SUM(amount) OVER(PARTITION BY product_link) / date_part AS average_per_day
                    FROM sales JOIN datediff ON true
                    ORDER BY average_per_day DESC
            ) top_products
            JOIN products ON top_products.product_link = products.link;
    ''')

    for i in cursor.fetchall():
        print(i)


def task_1_5(connection):   # 1.5.	Два лучших филиала согласно суммарному количеству проданного товара за всю историю наблюдений в регионе Урал по городу Екатеринбург с суммой продаж за январь
    cursor = connection.cursor()
    cursor.execute('''
        SELECT
            DISTINCT branches.name,
            SUM(sales.amount) OVER(PARTITION BY branches.link) AS amount_sum,
            SUM(CASE WHEN EXTRACT(MONTH FROM sales.timestamp) = 1 THEN sales.revenue END) OVER(PARTITION BY branches.link) AS jan_revenue
            FROM sales
            JOIN branches ON sales.branch_link = branches.link
            JOIN cities ON branches.city_link = cities.link
            WHERE cities.name = 'Екатеринбург'
            ORDER BY amount_sum DESC
            LIMIT 2;
    ''')

    for i in cursor.fetchall():
        print(i)


def task_1_6(connection):   # 1.6.	Требуется рассчитать и вывести в какие часы и в какой день недели происходит максимальное количество продаж
    cursor = connection.cursor()
    cursor.execute('''
        WITH dow_hours AS (
            SELECT
                id,
                EXTRACT(DOW FROM timestamp) AS dow,
                EXTRACT(HOUR FROM timestamp) AS hrs
                FROM sales
        )
        SELECT DISTINCT
            dow_hours.dow,
            dow_hours.hrs,
            SUM(sales.amount) OVER(PARTITION BY dow_hours.dow, dow_hours.hrs) as amount_sum
            FROM sales JOIN dow_hours ON sales.id = dow_hours.id
            ORDER BY amount_sum DESC;
    ''')

    for i in cursor.fetchall():
        print(i)


if __name__ == "__main__":
    connection = psycopg2.connect("dbname=postgres user=postgres password=rotas")
    user_input = ''
    print('Process started. Available commands: createall, dropall, fillall, whipe, sql, task, q, quit, exit.')

    while True:
        user_input = input()
        if user_input == 'createall':
            begin_time = time.time()
            create_database(connection)
            end_time = time.time()
            print('time elapsed: ', end_time - begin_time)

        elif user_input == 'dropall':
            begin_time = time.time()
            drop_database(connection)
            end_time = time.time()
            print('time elapsed: ', end_time - begin_time)

        elif user_input == 'fillall':
            begin_time = time.time()
            fill_tables(connection)
            end_time = time.time()
            print('time elapsed: ', end_time - begin_time)

        elif user_input == 'whipe':
            name = input('Table name: ')
            begin_time = time.time()
            whipe_table(connection, name)
            end_time = time.time()
            print('time elapsed: ', end_time - begin_time)
        
        elif user_input == 'sql':
            try:
                command = input('>>> ')
                begin_time = time.time()
                cursor = connection.cursor()
                cursor.execute(command)
                print(cursor.fetchall())
                end_time = time.time()
                print('time elapsed: ', end_time - begin_time)
            except Exception:
                print('Invalid sql query')
        
        elif user_input == 'task':
            begin_time = time.time()
            # task_1_1(connection)      # какой запрос из аналитической части нужно посмотреть, ту строчку раскоментируй
            task_1_2(connection)      # резултаты первого и второго записываются в бд в таблицы branches и cities соотвественно
            # task_1_3(connection)
            # task_1_4(connection)
            # task_1_5(connection)
            # task_1_6(connection)
            end_time = time.time()
            print('time elapsed: ', end_time - begin_time)

        elif user_input in ['q', 'quit', 'exit']:
            break
        else:
            print('Invalid command.')
    

    connection.commit()
    connection.close()


