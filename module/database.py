import pymysql

class Database:
    def connect(self):
        return pymysql.connect(host="localhost", user="root", password="admin",database="retail",charset="latin1")
    
    def readCustomer(self, id):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        print(id)
        try:
            cursor.execute(f"SELECT * FROM customers where customer_id = {id}")
            return cursor.fetchall()
        except Exception as e:
            return ()
        finally:
            con.close

    def readSubtotalByOneDepartment(self, id):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        print(id)
        try:
            cursor.execute(f'''
            select
                d.department_id ,
                d.department_name,
                round(sum(a.order_item_subtotal),2) as subtotal
            from 
            order_items a 
            inner join products b on a.order_item_product_id=b.product_id
            inner join categories c on b.product_category_id=c.category_id
            inner join departments d on c.category_department_id=d.department_id
            where d.department_id={id}
            group by  d.department_id ,d.department_name
            ''')
            return cursor.fetchall()
        except Exception as e:
            return ()
        finally:
            con.close

    def readSubtotalByDepartment(self):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        print(id)
        try:
            cursor.execute(f'''
            select
                d.department_id ,
                d.department_name,
                round(sum(a.order_item_subtotal),2) as subtotal
            from 
            order_items a 
            inner join products b on a.order_item_product_id=b.product_id
            inner join categories c on b.product_category_id=c.category_id
            inner join departments d on c.category_department_id=d.department_id
            group by  d.department_id ,d.department_name
            ''')
            return cursor.fetchall()
        except Exception as e:
            return ()
        finally:
            con.close
    
    def readQuantityByCategory(self):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(f'''
            select
                c.category_id,
                c.category_name,
                sum(a.order_item_quantity) as order_item_quantity
            from order_items a 
            inner join products b on a.order_item_product_id=b.product_id
            inner join categories c on b.product_category_id=c.category_id
            inner join departments d on c.category_department_id=d.department_id
            group by  c.category_id, c.category_name
            order by sum(a.order_item_quantity) desc
            ''')
            return cursor.fetchall()
        except Exception as e:
            return ()
        finally:
            con.close

    def readTopTenCustomers(self):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(f'''
            select
                f.customer_id,
                f.customer_fname,
                f.customer_lname,
                sum(a.order_item_quantity) as order_item_quantity
            from 
            order_items a 
            inner join products b on a.order_item_product_id=b.product_id
            inner join categories c on b.product_category_id=c.category_id
            inner join departments d on c.category_department_id=d.department_id
            inner join orders e on a.order_item_id=e.order_id
            inner join customers f on e.order_customer_id=f.customer_id
            group by  f.customer_fname,f.customer_lname
            order by sum(a.order_item_quantity) desc
            limit 10;
            ''')
            return cursor.fetchall()
        except Exception as e:
            return ()
        finally:
            con.close


    def readTopTenProducts(self):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(f'''
            select
            b.product_id,
            b.product_name,
            round(sum(a.order_item_subtotal),2) as order_item_subtotal
            from 
            order_items a 
            inner join products b on a.order_item_product_id=b.product_id
            inner join categories c on b.product_category_id=c.category_id
            inner join departments d on c.category_department_id=d.department_id
            group by  c.category_id, c.category_name
            order by sum(a.order_item_subtotal) desc
            limit 10
            ''')
            return cursor.fetchall()
        except Exception as e:
            return ()
        finally:
            con.close
    