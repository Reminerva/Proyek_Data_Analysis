import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from babel.numbers import format_currency

# Import File CSV
df_customer = pd.read_csv('data/df_customer_clean.csv')
df_order = pd.read_csv('data/df_order_clean.csv')
df_order_items = pd.read_csv('data/df_order_items_clean.csv')
df_order_payments = pd.read_csv('data/df_order_payments_clean.csv')
df_product = pd.read_csv('data/df_product_clean.csv')
df_sellers = pd.read_csv('data/df_sellers_clean.csv')

# print(df_customer.info())
# print(df_order.info()) 
# print(df_order_items.info()) 
# print(df_order_payments.info())
# print(df_product.info())
# print(df_sellers.info())

## Convert datetime
df_order['order_purchase_timestamp'] = pd.to_datetime(df_order['order_purchase_timestamp'])
df_order_items['shipping_limit_date'] = pd.to_datetime(df_order_items['shipping_limit_date'])

## Kumpulan Fungsi

### Mendapatkan pivot_seller dan pivot_order
def create_pivot_seller_and_order(df_order_items: pd.DataFrame,
                                  df_product: pd.DataFrame,
                                  df_order_payments: pd.DataFrame,
                                  df_order: pd.DataFrame) -> tuple:

    """
    Fungsi ini bertujuan untuk menghasilkan Data Frame pivot_seller dan pivot_order

    Parameters:
        df_order_items (pandas DataFrame): Data Frame df_order_items
        df_product (pandas DataFrame): Data Frame df_product
        df_order_payments (pandas DataFrame): Data Frame df_order_payments
        df_order (pandas DataFrame): Data Frame df_order

    Returns:
        tuple(pivot_seller, pivot_order):
        Data Frame pivot_seller dan  Data Frame pivot_order        
    """

    kelompok_cancel_unav = pd.concat([df_order[df_order['order_status']=='canceled']['order_id'],
                                    df_order[df_order['order_status']=='unavailable']['order_id']])

    kelompok_seller = pd.concat([df_order[df_order['order_status']=='delivered']['order_id'],
                            df_order[df_order['order_status']=='invoiced']['order_id'],
                                df_order[df_order['order_status']=='shipped']['order_id'],
                                df_order[df_order['order_status']=='processing']['order_id'],
                                df_order[df_order['order_status']=='created']['order_id'],
                                df_order[df_order['order_status']=='approved']['order_id']])

    kelompok_customer = pd.concat([df_order[df_order['order_status']=='delivered']['order_id'],
                             df_order[df_order['order_status']=='shipped']['order_id'],
                             df_order[df_order['order_status']=='invoiced']['order_id'],
                             df_order[df_order['order_status']=='processing']['order_id'],
                             df_order[df_order['order_status']=='created']['order_id'],
                             df_order[df_order['order_status']=='approved']['order_id']])
    
    def create_pivot_seller(df_order_items, df_product):

        df_temp = pd.merge(df_order_items, df_product, on='product_id', how='inner')
        pivot_seller = df_temp[df_temp['order_id'].isin(kelompok_seller)].groupby(by='seller_id').agg({
                                                                    'price': ['sum','mean','max', 'min'],
                                                                    'freight_value': ['sum','mean','max', 'min'],
                                                                    'product_id' : lambda x: list(x),
                                                                    'product_category_name' : lambda x: list(x),
                                                                    'shipping_limit_date' : lambda x: list(x)
                                                                    }).sort_values(by=('price','sum'), ascending=False)
        pivot_seller.columns = ['_'.join(col).strip() for col in pivot_seller.columns.values]
        
        return pivot_seller
    
    def create_pivot_order(df_order_payments, df_order):

        df_temp = df_order_items.drop(columns=['order_item_id','shipping_limit_date','seller_id'])
        df_temp = pd.merge(df_temp, df_product, on='product_id', how='inner')
        df_temp = df_temp[df_temp['order_id'].isin(kelompok_customer)].groupby(by='order_id').agg({
                                                                    'price': ['sum','mean','max', 'min'],
                                                                    'freight_value': ['sum','mean','max', 'min'],
                                                                    'product_id' : lambda x: list(x),
                                                                    'product_category_name' : lambda x: list(x)
                                                                    }).sort_values(by=('price','sum'), ascending=False)
        df_temp.columns = ['_'.join(col).strip() for col in df_temp.columns.values]
        
        pivot_order = df_order_payments[df_order_payments['order_id'].isin(kelompok_customer)].groupby(by='order_id').agg({
                                                            'payment_value': ['mean','max', 'min','sum'],
                                                            }).sort_values(by=('payment_value','sum'), ascending=False)
        pivot_order.columns = ['_'.join(col).strip() for col in pivot_order.columns.values]
        pivot_order = pd.merge(pivot_order, df_temp, on='order_id', how='inner')
        
        return pivot_order
    
    return create_pivot_seller(df_order_items, df_product), create_pivot_order(df_order_payments, df_order)

### Mendapatkan df_sellers_merged dan df_customer_merged
def create_df_sellers_and_customer_merged(pivot_seller: pd.DataFrame,
                                          df_sellers: pd.DataFrame,
                                          pivot_order: pd.DataFrame,
                                          df_order: pd.DataFrame,
                                          df_customer: pd.DataFrame) -> tuple:
    
    """
    Fungsi ini bertujuan untuk menghasilkan Data Frame df_sellers_merged dan df_customer_merged

    Parameters:
        pivot_seller (pandas DataFrame): Data Frame pivot_seller
        df_sellers (pandas DataFrame): Data Frame df_sellers
        pivot_order (pandas DataFrame): Data Frame pivot_order
        df_order (pandas DataFrame): Data Frame df_order
        df_customer (pandas DataFrame): Data Frame df_customer

    Returns:
        tuple(df_sellers_merged, df_customer_merged):
        Data Frame df_sellers_merged dan Data Frame df_customer_merged        
    """

    df_sellers_merged = pd.merge(pivot_seller, df_sellers, on='seller_id', how='inner')

    df_order_merged = pd.merge(pivot_order, df_order, on='order_id', how='inner')
    cols = df_order_merged.columns.tolist()
    cols.insert(1, cols.pop(cols.index('customer_id')))
    df_order_merged = df_order_merged[cols]

    df_customer_merged = pd.merge(df_order_merged, df_customer, on='customer_id', how='inner')

    return df_sellers_merged, df_customer_merged

### Mendapatkan df_sellers_city_merged
def create_df_sellers_city_merged(df_sellers_merged: pd.DataFrame) -> pd.DataFrame:

    """
    Fungsi ini bertujuan untuk menghasilkan 8 kota berpenghasilan terbesar beserta kategori barang yang terjual 
    yang disajikan ke Data Frame create_df_sellers_city_merged

    Parameters:
        df_sellers_merged (pandas DataFrame): Data Frame df_sellers_merged

    Returns:
        df_sellers_city_merged: Data Frame df_sellers_city_merged
    """    

    df_sellers_city_merged = df_sellers_merged.groupby(by='seller_city').agg({
                                                    'price_sum': 'sum',
                                                    'product_category_name_<lambda>': 'sum'
                                                    }).sort_values(by = ('price_sum'), ascending = False).head(8)
    
    return df_sellers_city_merged

### Mendapatkan kategori barang yang banyak dijual di kota berpenghasilan tertinggi
def return_kategori_di_kota_jual(df_sellers_city_merged: pd.DataFrame) -> list:
    
    """
    Fungsi ini bertujuan untuk menghasilkan 10 kategori yang terjual terbanyak di 8 kota berpenghasilan terbesar
    disajikan ke sebuah list yang berisi nama kota dan data frame yang berisi kategori-kategori barang beserta nominalnya:
    
    penjualan_kategoribarang_di_kota = [[kota_1, DataFrame],...,[kota_8, DataFrame]]
    
    Parameters:
        df_sellers_city_merged (pandas Data Frame): Data Frame df_sellers_city_merged

    Returns:
        penjualan_kategoribarang_di_kota (list): 
        list penjualan_kategoribarang_di_kota = [[kota_1, DataFrame],...,[kota_8, DataFrame]]
    """

    penjualan_kategoribarang_di_kota = []
    for i in range(8):
        df_temp = pd.DataFrame(df_sellers_city_merged['product_category_name_<lambda>'].iloc[i],
                            columns=['product_category_name_<lambda>']
                            ).value_counts().head(10)
        penjualan_kategoribarang_di_kota.append([df_sellers_city_merged.index[i],df_temp])

    return penjualan_kategoribarang_di_kota

### Mendapatkan df_customer_city_merged
def create_df_customer_city_merged(df_customer_merged: pd.DataFrame) -> pd.DataFrame:

    """
    Fungsi ini bertujuan untuk menghasilkan 8 kota berpengeluaran terbesar beserta kategori barang yang dibeli 
    yang disajikan ke Data Frame create_df_customer_city_merged

    Parameters:
        df_customer_merged (pandas DataFrame): Data Frame df_customer_merged

    Returns:
        df_customer_city_merged (pandas DataFrame): Data Frame df_customer_city_merged
    """    

    df_customer_city_merged = df_customer_merged.groupby(by='customer_city').agg({
                                                        'payment_value_sum': 'sum',
                                                        'product_category_name_<lambda>': 'sum'
                                                        }).sort_values(by = ('payment_value_sum'), ascending = False).head(8)
    
    return df_customer_city_merged

### Mendapatkan kategori barang yang banyak dibeli di kota berpengeluaran tertinggi
def return_kategori_di_kota_beli(df_customer_city_merged: pd.DataFrame) -> list:
    
    """
    Fungsi ini bertujuan untuk menghasilkan 10 kategori yang dibeli terbanyak di 8 kota berpengeluaran terbesar
    disajikan ke sebuah list yang berisi nama kota dan data frame yang berisi kategori-kategori barang beserta nominalnya:
    
    pemberlian_kategoribarang_di_kota = [[kota_1, DataFrame],...,[kota_8, DataFrame]]
    
    Parameters:
        df_customer_city_merged (pandas Data Frame): Data Frame df_customer_city_merged

    Returns:
        pemberlian_kategoribarang_di_kota (list): 
        list pemberlian_kategoribarang_di_kota = [[kota_1, DataFrame],...,[kota_8, DataFrame]]
    """

    pemberlian_kategoribarang_di_kota = []
    for i in range(8):
        df_temp = pd.DataFrame(df_customer_city_merged['product_category_name_<lambda>'].iloc[i],
                            columns=['product_category_name_<lambda>']
                            ).value_counts().head(10)
        pemberlian_kategoribarang_di_kota.append([df_customer_city_merged.index[i],df_temp])

    return pemberlian_kategoribarang_di_kota

### Membuat Klaster customer
kumpulan_klaster = ['Klaster I','Klaster II','Klaster III','Klaster IV','Klaster V','Klaster VI','Klaster VII']

def create_klaster_customer(df_customer_merged: pd.DataFrame) -> pd.DataFrame:

    """
    Fungsi ini digunakan untuk menghasilkan klaster customer

    Parameters:
        df_customer_merged (pandas Data Frames): Data Frame df_customer_merged
    
    Returns:
        df_customer_klaster (pandas DataFrame): Data Frame df_customer_klaster
    """

    batas_atas_klaster_I = 70
    batas_atas_klaster_II = 130
    batas_atas_klaster_III = 210
    batas_atas_klaster_IV = 350
    batas_atas_klaster_V = 1000
    batas_atas_klaster_VI = 4000

    batasan_klaster = [0,
                       batas_atas_klaster_I,
                       batas_atas_klaster_II,
                       batas_atas_klaster_III,
                       batas_atas_klaster_IV,
                       batas_atas_klaster_V,
                       batas_atas_klaster_VI,
                       float('inf')
                       ]

    df_customer_merged['Klaster'] = pd.cut(df_customer_merged['payment_value_sum'],
                                           bins=batasan_klaster,
                                           labels=kumpulan_klaster,
                                           include_lowest=True)
    df_customer_klaster = df_customer_merged[['customer_id','payment_value_sum','Klaster']]

    df_customer_klaster = df_customer_klaster.groupby(by='Klaster').agg({'Klaster': 'count'
                                                                        ,'customer_id': lambda x: list(x)
                                                                        ,'payment_value_sum': lambda x: list(x)})

    df_customer_klaster.columns = ['customer_id_count','customer_id','payment_value_sum']

    return df_customer_klaster

### Membuat Klaster seller
def create_klaster_sellers(df_sellers_merged: pd.DataFrame) -> pd.DataFrame:

    """
    Fungsi ini digunakan untuk menghasilkan klaster seller

    Parameters:
        df_sellers_merged (pandas Data Frames): Data Frame df_sellers_merged
    
    Returns:
        df_sellers_klaster (pandas DataFrame): Data Frame df_sellers_klaster
    """

    batas_atas_klaster_I = 300
    batas_atas_klaster_II = 1000
    batas_atas_klaster_III = 2500
    batas_atas_klaster_IV = 5000
    batas_atas_klaster_V = 10000
    batas_atas_klaster_VI = 50000

    batasan_klaster = [0,
                       batas_atas_klaster_I,
                       batas_atas_klaster_II,
                       batas_atas_klaster_III,
                       batas_atas_klaster_IV,
                       batas_atas_klaster_V,
                       batas_atas_klaster_VI,
                       float('inf')
                       ]

    df_sellers_merged['Klaster'] = pd.cut(df_sellers_merged['price_sum'],
                                          bins=batasan_klaster,
                                          labels=kumpulan_klaster,
                                          include_lowest=True)
    
    df_sellers_klaster = df_sellers_merged[['seller_id','price_sum','Klaster']]

    df_sellers_klaster = df_sellers_klaster.groupby(by='Klaster').agg({'Klaster': 'count'
                                                                    ,'seller_id': lambda x: list(x)
                                                                    ,'price_sum': lambda x: list(x)})

    df_sellers_klaster.columns = ['seller_id_count','seller_id','price_sum']

    return df_sellers_klaster

## MEMBUAT FILTER
min_date = df_order["order_purchase_timestamp"].min()
max_date = df_order["order_purchase_timestamp"].max()

with st.sidebar:
    st.title('Proyek Data Analisis')
    # Menambahkan logo perusahaan
    st.image("https://learn.g2.com/hubfs/Imported%20sitepage%20images/1ZB5giUShe0gw9a6L69qAgsd7wKTQ60ZRoJC5Xq3BIXS517sL6i6mnkAN9khqnaIGzE6FASAusRr7w=w1439-h786.png")

    # Mengambil start_date & end_date dari date_input
    start_date, end_date = st.date_input(
        label='Rentang Waktu',
        min_value=min_date,
        max_value=max_date,
        value=[min_date, max_date]
    )


### Filter diterapkan
df_order_update = df_order[(df_order["order_purchase_timestamp"] >= str(start_date)) &
                           (df_order["order_purchase_timestamp"] <= str(end_date))]
                           
df_order_items_update = df_order_items[(df_order_items["shipping_limit_date"] >= str(start_date)) &
                                       (df_order_items["shipping_limit_date"] <= str(end_date))]

### Data yang telah difilter diterapkan untuk membuat beberapa data frame
pivot_seller, pivot_order = create_pivot_seller_and_order(df_order_items_update,
                                                          df_product,
                                                          df_order_payments,
                                                          df_order_update)

df_sellers_merged, df_customer_merged = create_df_sellers_and_customer_merged(pivot_seller,
                                                                              df_sellers,
                                                                              pivot_order,
                                                                              df_order_update,
                                                                              df_customer)

df_sellers_city_merged = create_df_sellers_city_merged(df_sellers_merged)

penjualan_kategoribarang_di_kota = return_kategori_di_kota_jual(df_sellers_city_merged)

df_customer_city_merged = create_df_customer_city_merged(df_customer_merged)

pembelian_kategoribarang_di_kota = return_kategori_di_kota_jual(df_customer_city_merged)

df_customer_klaster = create_klaster_customer(df_customer_merged)

df_sellers_klaster = create_klaster_sellers(df_sellers_merged)

## DEPLOYMENT
st.title('Proyek Data Analisis :sparkles:')
# st.header('Proyek Data Analisis :sparkles:')
st.caption('Created by: Reksa Alamsyah')

st.write(
    """
    Proyek ini merupakan proyek analisis terhadap data E-Commerce yang didapatkan dari:
    
    https://drive.google.com/file/d/1MsAjPM7oKtVfJL_wRp1qmCajtSG1mdcK/view
    
    Data yang disajikan disini telah melalui tahapan-tahapan data cleaning sehingga siap untuk dianalisis.
    Data Cleaning dan Analisis dilakukan terlebih dahulu pada Google Colab dengan tujuan menjawab 8 pertanyaan utama.
    Google Colab tersebut dapat diakses pada link sebagai berikut:

    https://colab.research.google.com/drive/1nEoGv81s4V6xQXyWLrH9mi97WQuH8pmz?usp=sharing
    
    Analisis dimulai dengan mendapatkan performa seller dan customer di suatu kota dan state.
    """
)


col1, col2 = st.columns(2)

#### Pertanyaan 1
with col1:

    fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(12,20))

    df_0 = df_customer_merged.groupby(by='customer_state').agg({
                                                        'payment_value_sum': 'sum'
                                                        }).sort_values(by = ('payment_value_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('payment_value_sum'), ascending = False)
    
    colors = ["#8F4700", "#D3D3D3","#D3D3D3", "#D3D3D3", "#D3D3D3"]

    sns.barplot(y=df_0.index,
                x=df_0['payment_value_sum'],
                data=df_0,
                palette=colors,
                ax=ax[0]
                )
    ax[0].set_xlabel("Total Pengeluaran (Juta BRL)", fontsize=24)
    ax[0].set_ylabel("Nama State", fontsize=24)
    ax[0].set_title("Top 5 Total Pengeluaran Seluruh Customer di Setiap State", fontsize=28)
    ax[0].tick_params(axis='y', labelsize=20)
    ax[0].tick_params(axis='x', labelsize=20)

    df_0 = df_customer_merged.groupby(by='customer_city').agg({
                                                        'payment_value_sum': 'sum'
                                                        }).sort_values(by = ('payment_value_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('payment_value_sum'), ascending = False)
    
    sns.barplot(y=df_0.index,
                x=df_0['payment_value_sum'],
                data=df_0,
                palette=colors,
                ax=ax[1]
                )    
    
    ax[1].set_xlabel("Total Pengeluaran (Juta BRL)", fontsize=24)
    ax[1].set_ylabel("Nama City", fontsize=24)
    ax[1].set_title("Top 5 Total Pengeluaran Seluruh Customer di Setiap Kota", fontsize=28)
    ax[1].tick_params(axis='y', labelsize=20)
    ax[1].tick_params(axis='x', labelsize=20)
    
    df_0 = df_customer_merged.groupby(by='customer_id').agg({
                                                        'payment_value_sum': 'sum'
                                                        }).sort_values(by = ('payment_value_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('payment_value_sum'), ascending = False)
    
    index_ = [i[:3]+'...' for i in df_0.index]

    sns.barplot(y=index_,
                x=df_0['payment_value_sum'],
                data=df_0,
                palette=colors,
                ax=ax[2]
                )   

    ax[2].set_xlabel("Total Pengeluaran (BRL)", fontsize=24)
    ax[2].set_ylabel("ID Customer", fontsize=24)
    ax[2].set_title("Top 5 Total Pengeluaran Customer", fontsize=28)
    ax[2].tick_params(axis='y', labelsize=20)
    ax[2].tick_params(axis='x', labelsize=20)

    plt.tight_layout()

    st.pyplot(fig)

#### Pertanyaan 2
with col2:

    fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(12,20))

    df_0 = df_sellers_merged.groupby(by='seller_state').agg({
                                                        'price_sum': 'sum'
                                                        }).sort_values(by = ('price_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('price_sum'), ascending = False)
    
    colors = ["#8F4700", "#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3"]

    sns.barplot(y=df_0.index,
                x=df_0['price_sum'],
                data=df_0,
                palette=colors,
                ax=ax[0]
                )
                
    ax[0].set_xlabel("Total Penghasilan (Juta BRL)", fontsize=24)
    ax[0].set_ylabel("Nama State", fontsize=24)
    ax[0].set_title("Top 5 Total Penghasilan Seluruh Seller di Setiap State", fontsize=28)
    ax[0].tick_params(axis='y', labelsize=20)
    ax[0].tick_params(axis='x', labelsize=20)

    df_0 = df_sellers_merged.groupby(by='seller_city').agg({
                                                        'price_sum': 'sum'
                                                        }).sort_values(by = ('price_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('price_sum'), ascending = False)
    
    sns.barplot(y=df_0.index,
                x=df_0['price_sum'],
                data=df_0,
                palette=colors,
                ax=ax[1]
                )    
    
    ax[1].set_xlabel("Total Penghasilan (Juta BRL)", fontsize=24)
    ax[1].set_ylabel("Nama City", fontsize=24)
    ax[1].set_title("Top 5 Total Penghasilan Seluruh Seller di Setiap Kota", fontsize=28)
    ax[1].tick_params(axis='y', labelsize=20)
    ax[1].tick_params(axis='x', labelsize=20)
    
    df_0 = df_sellers_merged.groupby(by='seller_id').agg({
                                                        'price_sum': 'sum'
                                                        }).sort_values(by = ('price_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('price_sum'), ascending = False)
    
    index_ = [i[:3]+'...' for i in df_0.index]

    sns.barplot(y=index_,
                x=df_0['price_sum'],
                data=df_0,
                palette=colors,
                ax=ax[2]
                )   

    ax[2].set_xlabel("Total Penghasilan (BRL)", fontsize=24)
    ax[2].set_ylabel("ID Seller", fontsize=24)
    ax[2].set_title("Top 5 Total Penghasilan Seller", fontsize=28)
    ax[2].tick_params(axis='y', labelsize=20)
    ax[2].tick_params(axis='x', labelsize=20)

    plt.tight_layout()

    st.pyplot(fig)

#### Pertanyaan 3
if str(start_date)[:10]==str(min_date)[:10] and str(end_date)[:10]==str(max_date)[:10]:
    st.write(
        """
        Jika dlihat dari pengeluaran dan penghasilan setiap kota, maka dapat diperoleh insight menarik sebagai berikut:
        1. Kota dengan pengeluaran yang tinggi bukan berarti kota tersebut memiliki penghasilan yang relatif tinggi juga, begitupun sebaliknya. 
        Artinya, banyak terjadi kasus dimana customer membeli barang ke seller di kota yang berbeda dari customer tersebut.
        2. Contohnya diperingkat ke 2 pada kota dengan pengeluaran terbesar adalah 
        Rio De Janeiro yang memiliki pengeluaran sebesar 1.133.999 namun memiliki penghasilan kurang dari setengahnya yaitu 340.725.
        3. Menimbang hal tersebut (poin 2), di kota Rio De Janeiro masih memiliki peluang untuk seller karena 
        kota tersebut memiliki daya konsumsi yang kuat namun daya konsumsi tersebut disalurkan pada kota lain. 
        Dari sini muncul pertanyaan berikutnya yaitu, apa yang harus dijual? Untuk menjawab pertanyaan ini diperlukan
        analisis performa kategori barang di setiap kota (khususnya Rio De Janeiro) 
        untuk mengetahui apa saja kategori barang yang banyak dibeli oleh customer
        namun hanya sedikit dijual oleh seller di kota tersebut.
        """
    )
    # Membuat kanvas
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))  # 1 baris, 2 kolom
    colors = ["#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3", "#8F4700"]

    # Grafik pertama
    df_0 = df_customer_merged.groupby(by='customer_city').agg({
                                                            'payment_value_sum': 'sum'
                                                            }).sort_values(by = ('payment_value_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('payment_value_sum'), ascending = True)
    ax1.barh(y=df_0.index,
            width=df_0['payment_value_sum'],
            align='center',
            color=colors
            )
    ax1.set_xlabel("Total Pengeluaran (Juta BRL)")
    ax1.set_title("Top 5 Total Pengeluaran Seluruh Customer di Setiap Kota")

    # Grafik kedua
    df_0 = df_sellers_merged.groupby(by='seller_city').agg({
                                                            'price_sum': 'sum'
                                                            }).sort_values(by = ('price_sum'), ascending = False).head(5)
    df_0 = df_0.sort_values(by = ('price_sum'), ascending = True)
    ax2.barh(y=df_0.index,
            width=df_0['price_sum'],
            align='center',
            color=colors
            )
    ax2.set_xlabel("Total Pendapatan (Juta BRL)")
    ax2.set_title("Top 5 Total Pendapatan Seluruh Seller di Setiap Kota")

    # Tampilkan grafik
    plt.tight_layout()  # Agar layout lebih rapi
    st.pyplot(plt)

else:
    pass

### Pertanyaan 4

st.write(
    """
    Analisis berikutnya merupakan analisis perfroma kategori barang di setiap kota.
    Tabel-tabel berikut disusun berdasarkan kota dengan pengeluaran atau penghasilan terbanyak.
    
    """
)

input_kota = st.selectbox(
    label="Berapa kota yang ditampilkan?",
    options=(2, 3, 4, 5, 6, 7, 8),
    index=2
)

input_barang = st.selectbox(
    label="Berapa kategori barang yang ditampilkan?",
    options=(2, 3, 4, 5, 6, 7, 8, 9, 10),
    index=8
)

figsize_y = 100/(8*10)

fig, ax = plt.subplots(nrows=input_kota, ncols=2, figsize=(25,figsize_y*(input_kota*input_barang)))    

colors = ["#8F4700", "#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3",
          "#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3"]

for i in range (input_kota):
        
    penjualan_kategoribarang_di_kota[i][1] = penjualan_kategoribarang_di_kota[i][1].reset_index().sort_values(by = ('count'), ascending = False)
    penjualan_kategoribarang_di_kota[i][1] = penjualan_kategoribarang_di_kota[i][1].head(input_barang)
    
    penjualan_kategoribarang_di_kota[i][1]['product_category_name_<lambda>'] = [i[:12]+'...' for i in penjualan_kategoribarang_di_kota[i][1]['product_category_name_<lambda>']]
    
    sns.barplot(y=penjualan_kategoribarang_di_kota[i][1]['product_category_name_<lambda>'],
                x=penjualan_kategoribarang_di_kota[i][1]['count'],
                data=penjualan_kategoribarang_di_kota[i][1],
                palette=colors,
                ax=ax[i][0]
                )
                                      
    ax[i][0].set_xlabel("Total Penjualan Barang (Satuan)", fontsize=24)
    ax[i][0].set_ylabel("Kategori Barang", fontsize=24)
    ax[i][0].set_title("Top 10 Penjualan Kategori Barang di"+ " " + penjualan_kategoribarang_di_kota[i][0], fontsize=28)
    ax[i][0].tick_params(axis='y', labelsize=20)
    ax[i][0].tick_params(axis='x', labelsize=20)
        
    pembelian_kategoribarang_di_kota[i][1] = pembelian_kategoribarang_di_kota[i][1].reset_index().sort_values(by = ('count'), ascending = False)
    pembelian_kategoribarang_di_kota[i][1] = pembelian_kategoribarang_di_kota[i][1].head(input_barang)
    
    pembelian_kategoribarang_di_kota[i][1]['product_category_name_<lambda>'] = [i[:12]+'...' for i in pembelian_kategoribarang_di_kota[i][1]['product_category_name_<lambda>']]
        
    sns.barplot(y=pembelian_kategoribarang_di_kota[i][1]['product_category_name_<lambda>'],
                x=pembelian_kategoribarang_di_kota[i][1]['count'],
                data=pembelian_kategoribarang_di_kota[i][1],
                palette=colors,
                ax=ax[i][1]
                )
                                      
    ax[i][1].set_xlabel("Total Pembelian Barang (Satuan)", fontsize=24)
    ax[i][1].set_ylabel("Kategori Barang", fontsize=24)
    ax[i][1].set_title("Top 10 Pembelian Kategori Barang di"+ " " + pembelian_kategoribarang_di_kota[i][0], fontsize=28)
    ax[i][1].tick_params(axis='y', labelsize=20)
    ax[i][1].tick_params(axis='x', labelsize=20)

plt.tight_layout()                                                                                                                                 
st.pyplot(fig)

## Klaster Customer

st.write(
    """
    Berikutnya pembuatan klaster secara manual untuk customer berdasarkan  pengeluaran yang dilakukan.
    Klasterisasi ini dibuat sedemikian sehingga semakin tinggi tingkatan klaster maka semakin sedikit anggotanya.
    Berikut besaran dari masing-masing klaster:

    * Penghasilan Seller Klaster I < 300 BRL (Brazilian Real)

    * 300 BRL <= Penghasilan Seller Klaster II < 1000 BRL 

    * 1000 BRL <= Penghasilan Seller Klaster III < 2500 BRL 

    * 2500 BRL <= Penghasilan Seller Klaster IV < 5000 BRL 

    * 5000 BRL <= Penghasilan Seller Klaster V < 10000 BRL 

    * 10000 BRL <= Penghasilan Seller Klaster VI < 50000 BRL 

    * 50000 BRL <= Penghasilan Seller Klaster VII.
    """
)

fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(12,20))

colors = ('#C46100', '#EF9234', '#F4B678', '#F9E0A2', '#F4B678', '#EF9234')
explode = (0.02, 0.03, 0.05, 0.07, 0.09, 0.11)

klaster = kumpulan_klaster[:5]
klaster.append("Klaster VI dan VII")
count = (df_customer_klaster['customer_id_count'][0],
         df_customer_klaster['customer_id_count'][1],
         df_customer_klaster['customer_id_count'][2],
         df_customer_klaster['customer_id_count'][3],
         df_customer_klaster['customer_id_count'][4],
         df_customer_klaster['customer_id_count'][5]+df_customer_klaster['customer_id_count'][6]
         )

ax.pie(
    x=count,
    labels=klaster,
    autopct='%1.1f%%',
    colors=colors,
    explode=explode,
    wedgeprops = {'width': 0.5}
    )
ax.set_title('Klaster Customer', fontsize=26)

st.pyplot(fig)

## Klaster Seller

st.write(
    """
    Sedangkan pembuatan klaster  untuk seller dibuat berdasarkan  penghasilan yang dilakukan.
    Klasterisasi ini dibuat sedemikian sehingga semakin tinggi tingkatan klaster maka semakin sedikit anggotanya.
    Berikut besaran dari masing-masing klaster:

    * Pengeluaran Customer Klaster I < 70 BRL (Brazilian Real)

    * 70 BRL <= Pengeluaran Customer Klaster II < 130 BRL 

    * 130 BRL <= Pengeluaran Customer Klaster III < 210 BRL 

    * 210 BRL <= Pengeluaran Customer Klaster IV < 350 BRL 

    * 350 BRL <= Pengeluaran Customer Klaster V < 1000 BRL 

    * 1000 BRL <= Pengeluaran Customer Klaster VI < 4000 BRL 

    * 4000 BRL <= Pengeluaran Customer Klaster VII.
    """
)

fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(12,20))

klaster = kumpulan_klaster
count = (df_sellers_klaster['seller_id_count'][0],
         df_sellers_klaster['seller_id_count'][1],
         df_sellers_klaster['seller_id_count'][2],
         df_sellers_klaster['seller_id_count'][3],
         df_sellers_klaster['seller_id_count'][4],
         df_sellers_klaster['seller_id_count'][5],
         df_sellers_klaster['seller_id_count'][6]
         )
colors = ('#8F4700', '#C46100', '#EF9234', '#F4B678', '#F9E0A2', '#F4B678', '#C46100')
explode = (0.02, 0.03, 0.05, 0.07, 0.09, 0.11, 0.13)
ax.pie(
    x=count,
    labels=klaster,
    autopct='%1.1f%%',
    colors=colors,
    explode=explode,
    wedgeprops = {'width': 0.5}
    )
ax.set_title("Klaster Seller", fontsize=26)

st.pyplot(fig)