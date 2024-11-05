# Import các thư viện cần thiết cho Data Transformation
import pandas as pd
import glob
import numpy as np
from datetime import datetime, time
def Shopee_Order(gcs_data_path):
    # Import dữ liệu Shopee_Order
    df = pd.read_excel(gcs_data_path, header = 0)
    
    # Xóa bỏ các cột không cần thiết
    df = df.drop(
        [
            'Tracking Number*',
            'Shipment Method',
            'Estimated Ship Out Date',
            'Ship Time',
            'Order Paid Time',
            'Parent SKU Reference No.',
            'Receiver Name',
            'Phone Number',
            'Delivery Address',
            'Town',
            'District',
            'City',
            'Province',
            'Country',
            'Zip Code',
            'Remark from buyer',
            'Order Complete Time',
            'Note'
        ],
        axis=1,
    )

    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
                        {
                            'Order Creation Date': 'Created_Date', 
                            'Order ID': 'Order_ID', 
                            'Order Status': 'Order_Status', 
                            'Cancel reason': 'Cancel_reason', 
                            'Return / Refund Status': 'Return_Refund_Status', 
                            'Shipping Option': 'Shipping_Option', 
                            'Product Name': 'Product_Name', 
                            'SKU Reference No.': 'SKU', 
                            'Variation Name': 'Variation_Name', 
                            'Original Price': 'Original_Price', 
                            'Deal Price': 'Deal_Price', 
                            'Returned quantity': 'Returned_quantity', 
                            'Product Subtotal': 'Product_Subtotal', 
                            'Seller Rebate': 'Seller_Rebate', 
                            'Seller Discount': 'Seller_Discount', 
                            'Shopee Rebate': 'Shopee_Rebate', 
                            'SKU Total Weight': 'SKU_Total_Weight', 
                            'No of product in order': 'No_of_product_in_order', 
                            'Order Total Weight': 'Order_Total_Weight', 
                            'Voucher Code': 'Voucher_Code', 
                            'Seller Voucher': 'Seller_Voucher', 
                            'Seller Absorbed Coin Cashback': 'Seller_Absorbed_Coin_Cashback', 
                            'Shopee Voucher': 'Shopee_Voucher', 
                            'Bundle Deal Indicator': 'Bundle_Deal_Indicator', 
                            'Shopee Bundle Discount': 'Shopee_Bundle_Discount', 
                            'Seller Bundle Discount': 'Seller_Bundle_Discount', 
                            'Shopee Coins Offset': 'Shopee_Coins_Offset', 
                            'Credit Card Discount Total': 'Credit_Card_Discount_Total', 
                            'Total Amount': 'Total_Amount', 
                            'Buyer Paid Shipping Fee': 'Buyer_Paid_Shipping_Fee', 
                            'Shipping Rebate Estimate': 'Shipping_Rebate_Estimate', 
                            'Reverse Shipping Fee': 'Reverse_Shipping_Fee', 
                            'Transaction Fee(Incl. GST)': 'Transaction_Fee', 
                            'Commission Fee (Incl. GST)': 'Commission_Fee', 
                            'Service Fee (incl. GST)': 'Service_Fee', 
                            'Grand Total': 'Grand_Total', 
                            'Estimated Shipping Fee': 'Estimated_Shipping_Fee',
                            'Username (Buyer)': 'Username'
                            }
                )

    # Lấy ngày, tháng, năm từ cột thời gian
    df['Created_Date'] = pd.to_datetime(df['Created_Date'], format='%Y-%m-%d %H:%M')
    df['Creation_Time'] = df['Created_Date'].dt.time
    df['Creation_Date'] = df['Created_Date'].dt.date

    # Xóa cột "Created_Date"
    df = df.drop('Created_Date', axis = 1)

    # Thay đổi data types tương ứng
    df = df.astype({
        "Order_ID": "string"
        })

    return df

def Laz_Order(gcs_data_path):
    df = pd.read_excel(gcs_data_path)

    # Tính toán Quantity cho từng nhóm Order-SKU
    quantity_df = df.groupby(['orderNumber', 'sellerSku']).size().reset_index(name='Quantity')

    # Right join df order và df quantity
    df = pd.merge(df, quantity_df, how='right', on=['orderNumber', 'sellerSku'])

    # Chuyển column `createTime` thành 2 columns chứa dữ liệu của Date và Time
    df['createTime'] = pd.to_datetime(df['createTime'], format="%d %b %Y %H:%M")
    df['Creation_Date'] = df['createTime'].dt.date
    df['Creation_Time'] = df['createTime'].dt.time

    # Chỉ giữ lại 1 row cho 1 cặp Order-SKU
    df['rn'] = df.sort_values('createTime').groupby(['orderNumber', 'sellerSku']).cumcount() + 1
    df = df[df['rn'] == 1]

    # Đổi tên cho những cột sử dụng
    df = df[[
        'orderNumber', 'sellerSku', 'deliveryType', 'Creation_Date', 'Creation_Time', 
        'customerName', 'payMethod', 'paidPrice', 'unitPrice', 'sellerDiscountTotal', 
        'shippingFee', 'itemName', 'status', 'buyerFailedDeliveryReturnInitiator', 'buyerFailedDeliveryReason', 'Quantity'
    ]].rename(columns={
        'orderNumber': 'Order_ID',
        'sellerSku': 'SKU',
        'deliveryType': 'Delivery_Type',
        'customerName': 'Customer_Name',
        'payMethod': 'Pay_Method',
        'paidPrice': 'Paid_Price',
        'unitPrice': 'Unit_Price',
        'sellerDiscountTotal': 'Seller_Discount_Total',
        'shippingFee': 'Shipping_Fee',
        'itemName': 'Item_Name',
        'status': 'Status',
        'buyerFailedDeliveryReturnInitiator': 'bfd_Return_Initiator',
        'buyerFailedDeliveryReason': 'bfd_Reason'
    })

    # Change data types
    df = df.astype({
        'Order_ID': 'str',
        'SKU': 'str',
        'Delivery_Type': 'str',
        'Customer_Name': 'str',
        'Pay_Method': 'str',
        'Paid_Price': 'float',
        'Unit_Price': 'float',
        'Seller_Discount_Total': 'float',
        'Shipping_Fee': 'float',
        'Item_Name': 'str',
        'Status': 'str',
        'bfd_Return_Initiator': 'str',
        'bfd_Reason': 'str',
        'Quantity': 'int64'
    })

    return df

def Laz_Mall_Order(gcs_data_path):
    df = pd.read_excel(gcs_data_path)

    # Tính toán Quantity cho từng nhóm Order-SKU
    quantity_df = df.groupby(['orderNumber', 'sellerSku']).size().reset_index(name='Quantity')

    # Right join df order và df quantity
    df = pd.merge(df, quantity_df, how='right', on=['orderNumber', 'sellerSku'])

    # Chuyển column `createTime` thành 2 columns chứa dữ liệu của Date và Time
    df['createTime'] = pd.to_datetime(df['createTime'], format="%d %b %Y %H:%M")
    df['Creation_Date'] = df['createTime'].dt.date
    df['Creation_Time'] = df['createTime'].dt.time

    # Chỉ giữ lại 1 row cho 1 cặp Order-SKU
    df['rn'] = df.sort_values('createTime').groupby(['orderNumber', 'sellerSku']).cumcount() + 1
    df = df[df['rn'] == 1]

    # Đổi tên cho những cột sử dụng
    df = df[[
        'orderNumber', 'sellerSku', 'deliveryType', 'Creation_Date', 'Creation_Time', 
        'customerName', 'payMethod', 'paidPrice', 'unitPrice', 'sellerDiscountTotal', 
        'shippingFee', 'itemName', 'status', 'buyerFailedDeliveryReturnInitiator', 'buyerFailedDeliveryReason', 'Quantity'
    ]].rename(columns={
        'orderNumber': 'Order_ID',
        'sellerSku': 'SKU',
        'deliveryType': 'Delivery_Type',
        'customerName': 'Customer_Name',
        'payMethod': 'Pay_Method',
        'paidPrice': 'Paid_Price',
        'unitPrice': 'Unit_Price',
        'sellerDiscountTotal': 'Seller_Discount_Total',
        'shippingFee': 'Shipping_Fee',
        'itemName': 'Item_Name',
        'status': 'Status',
        'buyerFailedDeliveryReturnInitiator': 'bfd_Return_Initiator',
        'buyerFailedDeliveryReason': 'bfd_Reason'
    })

    # Change data types
    df = df.astype({
        'Order_ID': 'str',
        'SKU': 'str',
        'Delivery_Type': 'str',
        'Customer_Name': 'str',
        'Pay_Method': 'str',
        'Paid_Price': 'float',
        'Unit_Price': 'float',
        'Seller_Discount_Total': 'float',
        'Shipping_Fee': 'float',
        'Item_Name': 'str',
        'Status': 'str',
        'bfd_Return_Initiator': 'str',
        'bfd_Reason': 'str',
        'Quantity': 'int64'
    })
    return df

def Overwrite_Vietful_Inventory(gcs_data_path):
    df = pd.read_excel(gcs_data_path)

    df = df[['SKU','Partner SKU','Variant SKU','Product Name','Sale Price']]

    df.rename(columns=
          {'Partner SKU':'Partner_SKU',
           'Variant SKU':'Variant_SKU',
           'Product Name':'Product_Name',
           'Sale Price':'Sale_Price',
           'Discounted Price':'Discounted_Price'}, inplace=True)
    
    df.astype({'SKU':'str',
            'Variant_SKU':'str',
            'Partner_SKU':'str',
            'Product_Name':'str',
            'Sale_Price':'float64',
            'Discountes_Price':'float64'})
    
    return df

def Overwrite_Dim_Combo(gcs_data_path):
    df = pd.read_excel(gcs_data_path, sheet_name='Sheet chuẩn ')

    df.rename(columns=
            {'Barcode combo':'Barcode_Combo',
            'SKU combo':'SKU_Combo',
            'SKU 1':'SKU1',
            'SKU 2':'SKU2',
            'SKU 3':'SKU3',
            'SKU 4':'SKU4',
            'SKU 5':'SKU5',
            'SKU 6':'SKU6'}, inplace=True)
    
    df.astype({'Barcode_Combo':'str',
            'SKU_Combo':'str',
            'SKU1':'str',
            'SKU2':'str',
            'SKU3':'str',
            'SKU4':'str',
            'SKU5':'str',
            'SKU6':'str'}, inplace=True)
    
    return df

def 

