# Import các thư viện cần thiết cho Data Transformation
import pandas as pd
import glob
import numpy as np
from datetime import datetime, time

def get_first_date_of_month(time_suffix):
    # Parse the first date of that month using strptime and strftime
    first_date = datetime.strptime(f"01{time_suffix}", "%d%b%Y").strftime("%Y-%m-%d")
    return first_date

def classify_response_time(time):
    if time < 60:
        return "Less than 1 hour"
    elif 60 <= time < 120:
        return "1-2 hours"
    else:
        return "More than 2 hours"

def clean_currency_string(currency_str):
    # Remove the currency symbol and commas
    cleaned_str = currency_str.replace('S$', '').replace(',', '').strip()
    
    # Convert the cleaned string to float
    try:
        return float(cleaned_str)
    except ValueError:
        return None

def convert_percentage(perc):
    # Function để chuyển dạng phần trăm sang dạng float
    if isinstance(perc, str):
        return float(perc.replace('%', '')) / 100
    return perc

def Outbound_Vietful(import_data_path, export_data_path, time_suffix):
    try:
        list = [pd.read_excel(filename) for filename in glob.glob(f"{import_data_path}\Outbound_Vietful_{time_suffix}*.xlsx")]
        df = pd.concat(list, axis=0)
    except:
        pd.read_excel(f"{import_data_path}\Outbound_Vietful_{time_suffix}.xlsx")
    df = df[['CreatedDate','ORCode','PartnerOrCode','SalesChannelCode',
        'ORStatus','SKU','PartnerSKU','OrderQty','ProductName',
        'Category','Discount Amount','Payment Amount',
        'PackageNo','BillOfLading']].rename(columns = 
                                            {'CreatedDate':'Created_Date',
                                             'ORCode':'OR_Code',
                                             'PartnerOrCode':'Partner_OR_Code',
                                             'SalesChannelCode':'Sales_Channel_Code',
                                             'ORStatus':'OR_Status',
                                             'PartnerSKU':'Partner_SKU',
                                             'OrderQty':'Order_Quantity',
                                             'ProductName':'Product_Name',
                                             'Discount Amount':'Discount_Amount',
                                             'Payment Amount':'Payment_Amount',
                                             'PackageNo':'Pakage_No',
                                             'BillOfLading':'Bill_Of_Lading'})
    df['Created_Date'] = pd.to_datetime(df['Created_Date'], errors='coerce').dt.date
    df['Sales_Channel_Code']=df['Sales_Channel_Code'].fillna('INSTORE')
    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Outbound_Vietful_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df)

def Dim_Product(import_data_path, export_data_path, time_suffix):
    try:
        list = [pd.read_excel(filename) for filename in glob.glob(f"{import_data_path}\Dim_Product_{time_suffix}*.xlsx")]
        df = pd.concat(list, axis=0)
    except:
        pd.read_excel(f"{import_data_path}\Dim_Product_{time_suffix}.xlsx")
    df = df.rename(columns=
                   {'Product Name':'Product_Name'})
    df = df.astype({'Product_Name': 'str',
                    'SKU':'str'})
    df = df.drop_duplicates(subset='SKU')
    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Dim_Product_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def TikTok_Key_Metrics(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Tiktok_Key_Metrics từ cả 2 account mới và cũ
    try:
        list = [pd.read_excel(filename, header = 4) for filename in glob.glob(f"{import_data_path}\TikTok_Key_Metrics_{time_suffix}*.xlsx")]
        df = pd.concat(list, axis=0)
    except:
        df = pd.read_excel(f"{import_data_path}\TikTok_Key_Metrics_{time_suffix}.xlsx", header = 4)

    # Xóa cột "Conversion rate", cột này sẽ được tính toán lại sau khi đã groupby và tính tổng theo ngày
    df = df.drop("Conversion rate", axis = "columns")

    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
               {
                'Date': 'Date',
                'GMV (S$)': 'GMV',
                'Refunds (S$)': 'Refunds',
                'Gross Revenue (with platform product subsidy)': 'Gross_Revenue',
                'Items sold': 'Items_Sold',
                'Buyers': 'Buyers',
                'Page views': 'Page_Views',
                'Visitors': 'Visitors',
                'SKU orders': 'SKU_Orders',
                'Orders': 'Orders' 
               })
    
    # groupby theo 'Date' và tính tổng các cột còn lại
    grouped_df = df.groupby('Date').sum()
    # reset index để biến column 'Date' thành column bình thường thày vì là index
    grouped_df.reset_index(inplace=True)

    # Thêm cột Conversion_Rate tính bằng công thức Conversion_Rate = Orders / Visitors
    grouped_df['Conversion_Rate'] = (grouped_df['Orders'] / grouped_df['Visitors'] * 100).round(2)

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = grouped_df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f'{export_data_path}\TikTok_Key_Metrics_{time_suffix}.json', 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def TikTok_Order(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Tiktok_Order từ cả 2 account mới và cũ
    try:
        list = [pd.read_csv(filename) for filename in glob.glob(f"{import_data_path}\TikTok_Order_{time_suffix}*.csv")]
        df = pd.concat(list, axis=0)
    except:
        df = pd.read_csv(f"{import_data_path}\TikTok_Order_{time_suffix}.csv")

    # Xóa những columns không cần thiết
    df = df.drop(
        [
        "Order Substatus", 
        "Cancelation/Return Type", 
        "SKU ID", 
        "Normal or Pre-order",
        # "Payment platform discount",
        "Paid Time", 
        "RTS Time", 
        "Shipped Time", 
        "Delivered Time", 
        "Cancelled Time", 
        "Fulfillment Type", 
        "Warehouse Name", 
        "Tracking ID", 
        "Delivery Option", 
        "Shipping Provider Name", 
        "Buyer Message", 
        "Recipient", 
        "Phone #", 
        "Country", 
        "Zipcode", 
        "Detail Address", 
        "Unit/floor", 
        "Additional address information", 
        "Weight(kg)", 
        "Product Category", 
        "Package ID", 
        "Seller Note", 
        "Checked Status", 
        "Checked Marked by"
        ],
        axis=1,
    )

    # Xóa "SGD " ở những cột chưa giá trị tiền, chuyển datatype của những cột đó về dạng float
    for column_name in [
        'SKU Unit Original Price',
        'SKU Subtotal Before Discount',
        'SKU Platform Discount',
        'SKU Seller Discount',
        'SKU Subtotal After Discount',
        'Shipping Fee After Discount',
        'Original Shipping Fee',
        'Shipping Fee Seller Discount',
        'Shipping Fee Platform Discount',
        'Taxes',
        'Order Amount',
        'Order Refund Amount'
    ]:
        df[column_name] = df[column_name].str.lstrip('SGD ').astype(float)

    # Tách cột "Created Time" ra thành cột "Created Date" (chỉ chứa ngày) và "Created Time" (chỉ chứa giờ)
    df['Created_Date'] = df['Created Time'].str[:10]
    df['Created_Time'] = df['Created Time'].str[11:19]
    # Xóa cột "Created Time"
    df = df.drop('Created Time', axis = 1)

    # Format cột "Created_Date" theo dạng '%Y-%m-%d'
    df['Created_Date'] = pd.to_datetime(df['Created_Date'], format='%d/%m/%Y')
    # Chuyển "Created_Date" thành datatype String để không bị lỗi save df ở dạng .json
    df['Created_Date'] = df['Created_Date'].dt.strftime('%Y-%m-%d')

    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
        {
            "Order ID": "Order_ID",
            "Order Status": "Order_Status",
            "Seller SKU": "SKU",
            "Product Name": "Product_Name",
            "Sku Quantity of return": "SKU_Quantity_of_return",
            "SKU Unit Original Price": "SKU_Unit_Original_Price",
            "SKU Subtotal Before Discount": "SKU_Subtotal_Before_Discount",
            "SKU Platform Discount": "SKU_Platform_Discount",
            "SKU Seller Discount": "SKU_Seller_Discount",
            "SKU Subtotal After Discount": "SKU_Subtotal_After_Discount",
            "Shipping Fee After Discount": "Shipping_Fee_After_Discount",
            "Original Shipping Fee": "Original_Shipping_Fee",
            "Shipping Fee Seller Discount": "Shipping_Fee_Seller_Discount",
            "Shipping Fee Platform Discount": "Shipping_Fee_Platform_Discount",
            "Order Amount": "Order_Amount",
            "Order Refund Amount": "Order_Refund_Amount",
            "Payment Method": "Payment_Method",
            "Cancel By": "Cancel_By",
            "Cancel Reason": "Cancel_Reason",
            "Buyer Username": "Buyer_Username"
        }
    )

    # Sắp xếp df theo thứ tự tăng dần của "Order_ID"
    df = df.sort_values("Order_ID")

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f'{export_data_path}\TikTok_Order_{time_suffix}.json', 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def TikTok_Return_Refund(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Tiktok_Return_Refund từ cả 2 account mới và cũ
    try:
        list = [pd.read_csv(filename) for filename in glob.glob(f"{import_data_path}\TikTok_Return_Refund_{time_suffix}*.csv")]
        df = pd.concat(list, axis=0)
    except:
        pd.read_csv(f"{import_data_path}\TikTok_Return_Refund_{time_suffix}.csv")

    # Xóa "S$" ở những cột chưa giá trị tiền, chuyển datatype của những cột đó về dạng float
    for column_name in [
        'Order Amount',
        'Return unit price'
    ]:
        df[column_name] = df[column_name].str.lstrip('S$').astype(float)

    # Xóa những columns không cần thiết
    df = df.drop(
        [
            "Dispute Status", 
            "Return Logistics Tracking ID",
            "Appeal Status",
            "Compensation Status",
            "Compensation Amount",
            "Buyer Note"
        ],
        axis=1,
    )

    # Thay thế tất cả kí tự "\t" thành "" để không bị lỗi khi tách dữ liệu từ các cột
    df = df.replace("\t", "", regex = True)

    # Tách cột "Refund Time" ra thành cột "Refund_Date" (chỉ chứa ngày) và "Refund_Time" (chỉ chứa giờ)
    df['Refund_Date'] = df['Refund Time'].str[:10]
    df['Refund_Time'] = df['Refund Time'].str[11:19]
    # Format cột "Refund_Date" theo dạng '%Y-%m-%d'
    df['Refund_Date'] = pd.to_datetime(df['Refund_Date'], format='%d/%m/%Y')
    # Chuyển "Created_Date" thành datatype String để không bị lỗi save df ở dạng .json
    df['Refund_Date'] = df['Refund_Date'].dt.strftime('%Y-%m-%d')
    # Xóa cột "Refund Time"
    df = df.drop('Refund Time', axis = 1)

    # Tách cột "Time Requested" ra thành cột "Day_Requested" (chỉ chứa ngày) và "Time_Requested" (chỉ chứa giờ)
    df['Day_Requested'] = df['Time Requested'].str[:10]
    df['Time_Requested'] = df['Time Requested'].str[11:19]
    # Format cột "Day_Requested" theo dạng '%Y-%m-%d'
    df['Day_Requested'] = pd.to_datetime(df['Day_Requested'], format='%d/%m/%Y')
    # Chuyển "Created_Date" thành datatype String để không bị lỗi save df ở dạng .json
    df['Day_Requested'] = df['Day_Requested'].dt.strftime('%Y-%m-%d')
    # Xóa cột "Time Requested"
    df = df.drop('Time Requested', axis = 1)

    # Đổi tên các cột cần thiết
    df = df.rename(columns={
        "Return Order ID": "Return_Order_ID",
        "Order ID": "Order_ID",
        "Order Amount": "Order_Amount",
        "Order Status": "Order_Status",
        "Order Substatus": "Order_Substatus",
        "Payment Method": "Payment_Method",
        "SKU ID": "SKU_ID",
        "Seller SKU": "Seller_SKU",
        "Product Name": "Product_Name",
        "SKU Name": "SKU_Name",
        "Buyer Username": "Buyer_Username",
        "Return Type": "Return_Type",
        "Return Reason": "Return_Reason",
        "Return unit price": "Return_unit_price",
        "Return Quantity": "Return_Quantity",
        "Return Status": "Return_Status",
        "Return Sub Status": "Return_Sub_Status"
    })

    # Sắp xếp df theo thứ tự tăng dần của "Return_Order_ID"
    df = df.sort_values("Return_Order_ID")

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f'{export_data_path}\TikTok_Return_Refund_{time_suffix}.json', 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Chat(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Shopee_Chat
    df = pd.read_excel(f"{import_data_path}\Shopee_Chat_{time_suffix}.xlsx", sheet_name='Metric Trends')
    
    # Xóa những cột không sử dụng
    df = df.drop('CSAT %', axis = 1)

    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
                            {
                                'Chat Enquired': 'Chat_Enquired',
                                'Visitors Enquired': 'Visitors_Enquired',
                                'Enquiry Rate': 'Enquiry_Rate',
                                'Responded Chats': 'Responded_Chats',
                                'Non-responded Chats': 'Non_Responded_Chats',
                                'Response Time': 'Response_Time',
                                'Conversion Rate (Enquire to Response)': 'Conversion_Rate_Enquire_to_Response',
                                'Chat Response Rate': 'Chat_Response_Rate',
                                'Sales (SGD)': 'Sales',
                                'Conversion Rate (Respond to Placed)': 'Conversion_Rate_Respond_to_Placed'
                            }

                    )
    
    # Chuyển đổi cột Enquiry_Rate sang dạng float
    df['Enquiry_Rate'] = df['Enquiry_Rate'].str.split('%').str[0]
    df['Enquiry_Rate'] = df['Enquiry_Rate'].astype(float)
    df['Enquiry_Rate'] = df['Enquiry_Rate']/100
    # Chuyển đổi cột Conversion_Rate sang dạng float
    df['Conversion_Rate_Enquire_to_Response'] = df['Conversion_Rate_Enquire_to_Response'].str.split('%').str[0]
    df['Conversion_Rate_Enquire_to_Response'] = df['Conversion_Rate_Enquire_to_Response'].astype(float)
    df['Conversion_Rate_Enquire_to_Response'] = df['Conversion_Rate_Enquire_to_Response']/100
    # Chuyển đổi cột Conversion_Rate sang dạng float
    df['Conversion_Rate_Respond_to_Placed'] = df['Conversion_Rate_Respond_to_Placed'].str.split('%').str[0]
    df['Conversion_Rate_Respond_to_Placed'] = df['Conversion_Rate_Respond_to_Placed'].astype(float)
    df['Conversion_Rate_Respond_to_Placed'] = df['Conversion_Rate_Respond_to_Placed']/100


    # Tính toán cột Response_Time theo phút và tạo thành cột mới là Reply_Minutes
    time = pd.DataFrame()
    time[['Hour', 'Minute', 'Second']] = df['Response_Time'].str.split(':', expand=True).astype(int)
    df['Reply_Minutes'] = time['Hour'] * 60 + time['Minute'] + time['Second'] / 60

    # Function để phân loại Reply_Minutes
    def categorize_reply_time(minutes):
        if minutes < 60:
            return 'Less than 1 hour'
        elif 60 <= minutes < 120:
            return '1-2 hours'
        else:
            return 'More than 2 hours'
    # Conditional formatting cho cột Reply_Minutes
    df['Time_Reply'] = df['Reply_Minutes'].apply(categorize_reply_time)

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Chat_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Key_Metrics(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Shopee_Key_Metrics
    df = pd.read_excel(f"{import_data_path}\Shopee_Key_Metrics_{time_suffix}.xlsx", header = 3)
    
    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
                            {
                                'Sales (SGD)': 'Sales', 
                                'Sales per Order': 'Sales_Per_Order', 
                                'Page Views': 'Page_Views', 
                                'Conversion Rate (by paid order)': 'Conversion_Rate_Paid_Order', 
                                'Cancelled Orders': 'Cancelled_Orders', 
                                'Cancelled Sales': 'Cancelled_Sales', 
                                'Returned/Refunded Orders': 'Returned_Refunded_Orders', 
                                'Returned/Refunded Sales': 'Returned_Refunded_Sales', 
                                '# of buyers': 'Num_of_buyers', 
                                '# of new buyers': 'Num_of_new_buyers', 
                                '# of existing buyers': 'Num_of_existing_buyers', 
                                '# of potential buyers': 'Num_of_potential_buyers', 
                                'Repeat Purchase Rate': 'Repeat_Purchase_Rate'
                            }
                    )

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Key_Metrics_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Order(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Shopee_Order
    list = [pd.read_excel(filename) for filename in glob.glob(f"{import_data_path}\Shopee_Order_{time_suffix}*.xlsx")]
    df = pd.concat(list, axis=0)
    
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
                            'Voucher Code From Seller': 'Voucher_Code', 
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
    df['Created_Date'] = df['Created_Date'].astype(str).str.split(' ').str[0]

    # fillna cột Returned_quantity với giá trị 0
    df['Returned_quantity'] = df['Returned_quantity'].fillna(0).astype(int)

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Order_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Return_Refund(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Shopee_Return_Refund
    df = pd.read_excel(f"{import_data_path}\Shopee_Return_Refund_{time_suffix}.xls", na_values='-')

    # Xóa bỏ các cột không cần thiết
    df = df.drop(
        [
        'Username (Buyer)',
        'Return Tracking Number',
        'Return Tracking Status',
        'Return Delivery Completed Time',
        'Province',
        'City',
        'Delivery Address',
        'Phone Number',
        'Return Pickup Address',
        'Province.1',
        'City.1',
        'Zip Code.1',
        'Return Pickup Phone Number',
        'Available to Raise Dispute by',
        'Dispute Reason',
        'Seller Remark for Dispute',
        ],
        axis=1,
    )

    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
                        {
                            'Return ID': 'Return_ID',
                            'Order ID': 'Order_ID',
                            'Order Creation Date': 'Order_Creation_Date',
                            'Product Name': 'Product_Name',
                            'Parent SKU': 'Parent_SKU',
                            'Variation Name': 'Variation_Name',
                            'Unit Price': 'Unit_Price',
                            'Return Creation Time': 'Return_Creation_Date',
                            'Return / Refund Status': 'Return_Refund_Status',
                            'Return Type': 'Return_Type',
                            'Return Quantity': 'Return_Quantity',
                            'Return/Refund Solution': 'Return_Refund_Solution',
                            'Return Reason': 'Return_Reason',
                            'Buyer Return Remarks': 'Buyer_Return_Remarks',
                            'Refund Total Amount': 'Refund_Total_Amount',
                            'Refund Completed Time': 'Refund_Completed_Date',
                            'Return to Shopee Warehouse': 'Return_to_Shopee_Warehouse',
                            'Return Shipping Option': 'Return_Shipping_Option',
                            'Zip Code': 'Zip_Code',
                            'Compensation Amount': 'Compensation_Amount',
                            'Order Total Amount': 'Order_Total_Amount',
                            'Payment Method': 'Payment_Method',
                            'Buyer Remark for Order': 'Buyer_Remark_for_Order'
                            }
                )
    
    # Lấy giờ, phút từ cột thời gian
    df['Order_Creation_Time'] = df['Order_Creation_Date'].str.split(' ').str[1]
    # Lấy ngày, tháng, năm từ cột thời gian
    df['Order_Creation_Date'] = df['Order_Creation_Date'].str.split(' ').str[0]
    # Add seconds to the 'Order_Creation_Time' column
    df['Order_Creation_Time'] = df['Order_Creation_Time'].apply(lambda x: f"{x}:00" if pd.notna(x) else x)

    # Lấy giờ, phút từ cột thời gian trả/ hoàn
    df['Return_Creation_Time'] = df['Return_Creation_Date'].str.split(' ').str[1]
    # Lấy ngày, tháng, năm từ cột thời gian trả/ hoàn
    df['Return_Creation_Date'] = df['Return_Creation_Date'].str.split(' ').str[0]
    # Add seconds to the 'Return_Creation_Time' column
    df['Return_Creation_Time'] = df['Return_Creation_Time'].apply(lambda x: f"{x}:00" if pd.notna(x) else x)

    # Lấy giờ, phút từ cột thời gian hoàn thành hoàn/trả
    df['Refund_Completed_Time'] = df['Refund_Completed_Date'].str.split(' ').str[1]
    # Lấy ngày, tháng, năm từ cột thời gian hoàn thành hoàn/trả
    df['Refund_Completed_Date'] = df['Refund_Completed_Date'].str.split(' ').str[0]
    # Add seconds to the 'Refund_Completed_Time' column
    df['Refund_Completed_Time'] = df['Refund_Completed_Time'].apply(lambda x: f"{x}:00" if pd.notna(x) else x)

    # Sắp xếp lại thứ tự các columns
    df = df.loc[:,
            [
                'Return_ID', 
                'Order_ID', 
                'Order_Creation_Date',
                'Order_Creation_Time',
                'Product_Name',
                'Parent_SKU', 
                'Variation_Name', 
                'SKU', 
                'Unit_Price',
                'Return_Creation_Date',
                'Return_Creation_Time',
                'Return_Refund_Status', 
                'Return_Type',
                'Return_Quantity', 
                'Return_Refund_Solution', 
                'Return_Reason',
                'Buyer_Return_Remarks', 
                'Refund_Total_Amount', 
                'Refund_Completed_Date',
                'Refund_Completed_Time',
                'Return_to_Shopee_Warehouse', 
                'Return_Shipping_Option', 
                'Zip_Code',
                'Compensation_Amount', 
                'Order_Total_Amount', 
                'Payment_Method',
                'Buyer_Remark_for_Order'
            ]
           ]

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Return_Refund_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Sales_Overview(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Shopee_Sales_Overview
    df = pd.read_excel(f"{import_data_path}\Shopee_Sales_Overview_{time_suffix}.xlsx", header = 3)

    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
                            {
                            'Visitors (Visit)': 'Visitors_Visit', 
                            'Buyers (Placed Orders)': 'Buyers_Placed_Orders', 
                            'Units (Placed Orders)': 'Units_Placed_Orders', 
                            'Orders (Placed Orders)': 'Orders_Placed_Orders', 
                            'Sales (Placed Orders) (SGD)': 'Sales_Placed_Orders', 
                            'Conversion Rate (Visit to Placed)': 'Conversion_Rate_Visitor_Placed_Order', 
                            'Buyers (Paid Orders)': 'Buyers_Paid_Orders', 
                            'Units (Paid Orders)': 'Units_Paid_Orders', 
                            'Orders (Paid Orders)': 'Orders_Paid_Orders', 
                            'Sales (Paid Orders) (SGD)': 'Sales_Paid_Orders', 
                            'Sales per Buyer (Paid Orders) (SGD)': 'Sales_per_Buyer_Paid_Orders', 
                            'Conversion Rate': 'Conversion_Rate', 
                            'Conversion Rate (Placed to Paid)': 'Conversion_Rate_Placed_to_Paid'
                            }
                    )
    
    # Chuyển đổi cột Conversion_Rate_Visitor_Placed_Order sang dạng float
    df['Conversion_Rate_Visitor_Placed_Order'] = df['Conversion_Rate_Visitor_Placed_Order'].str.split('%').str[0]
    df['Conversion_Rate_Visitor_Placed_Order'] = df['Conversion_Rate_Visitor_Placed_Order'].astype(float)
    df['Conversion_Rate_Visitor_Placed_Order'] = df['Conversion_Rate_Visitor_Placed_Order']/100
    # Chuyển đổi cột Conversion_Rate sang dạng float
    df['Conversion_Rate'] = df['Conversion_Rate'].str.split('%').str[0]
    df['Conversion_Rate'] = df['Conversion_Rate'].astype(float)
    df['Conversion_Rate'] = df['Conversion_Rate']/100
    # Chuyển đổi cột Conversion_Rate sang dạng float
    df['Conversion_Rate_Placed_to_Paid'] = df['Conversion_Rate_Placed_to_Paid'].str.split('%').str[0]
    df['Conversion_Rate_Placed_to_Paid'] = df['Conversion_Rate_Placed_to_Paid'].astype(float)
    df['Conversion_Rate_Placed_to_Paid'] = df['Conversion_Rate_Placed_to_Paid']/100

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Sales_Overview_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Traffic(import_data_path, export_data_path, time_suffix):
    # Import dữ liệu Shopee_Chat
    df = pd.read_excel(f"{import_data_path}\Shopee_Traffic_{time_suffix}.xlsx", sheet_name='All', header = 3)
    
    # Đổi tên các cột cần thiết
    df = df.rename(columns = 
                            {
                            'Page Views': 'Page_Views',
                            'Avg. Page Views': 'Avg_Page_Views',
                            'Avg. Time Spent': 'Avg_Time_Spent',
                            'Bounce Rate': 'Bounce_Rate',
                            'New Visitors': 'New_Visitors',
                            'Existing Visitors': 'Existing_Visitors',
                            'New Followers': 'New_Followers',
                            }
                    )

    # Chuyển đổi cột Enquiry_Rate sang dạng float
    df['Bounce_Rate'] = df['Bounce_Rate'].str.split('%').str[0]
    df['Bounce_Rate'] = df['Bounce_Rate'].astype(float)
    df['Bounce_Rate'] = df['Bounce_Rate']/100

    # Chuyển đổi thời gian sử dụng thành phút
    time = pd.DataFrame()
    time[['Hour', 'Minute', 'Second']] = df['Avg_Time_Spent'].str.split(':', expand=True).astype(int)
    df['Avg_Time_Spent'] = time['Hour'] * 60 + time['Minute'] + time['Second'] / 60

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Traffic_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Details_Chat(import_data_path, export_data_path, time_suffix):
    result_shopee_order = pd.read_json(f"{export_data_path}\Shopee_Order_{time_suffix}.json", lines = True)
    shopee_chat_details = pd.read_excel(f"{import_data_path}\Shopee_Chat_{time_suffix}.xlsx",sheet_name='Responded Chat Details')

    def calculate_minutes(time_str):
        try:
            # Split the string into hours, minutes, and seconds
            hours, minutes, seconds = map(int, time_str.split(':'))
            # Calculate total minutes
            total_minutes = hours * 60 + minutes + seconds / 60
            return total_minutes
        except ValueError:
            # Handle the case where the time_str format is incorrect
            return None
    
    #Merge Shopee Chat Details and Shopee Orders
    shopee_chat_merged_df = pd.merge(shopee_chat_details, result_shopee_order, left_on='Sender', right_on='Username', how='left')

    # Selecting specific columns if necessary
    shopee_details_chat = shopee_chat_merged_df[['Sender', 'Responded Timestamp', 'Response Time', 'Responded Message',
                                'Order_ID', 'Order_Status', 'Created_Date', 'Product_Name', 'SKU', 'Quantity',
                                'Total_Amount', 'No_of_product_in_order']].rename(columns=
                                                                                    {'Responded Timestamp':'Responded_Timestamp',
                                                                                    'Response Time':'Response_Time',
                                                                                    'Responded Message':'Responded_Message'
                                                                                    })
    # Rename Columns
    shopee_details_chat.rename(columns={'Created_Date': 'Order_Creation_Date'}, inplace=True)

    # Ensure date and time types are correct
    # Correct format to match both date and time parts of the string
    shopee_details_chat['Responded_Timestamp'] = pd.to_datetime(shopee_details_chat['Responded_Timestamp'], format='%d-%m-%Y %H:%M', errors='coerce')

    # Extracting the time part after conversion
    shopee_details_chat['Responded_Timestamp_Time'] = shopee_details_chat['Responded_Timestamp'].dt.time
    shopee_details_chat['Responded_Timestamp'] = pd.to_datetime(shopee_details_chat['Responded_Timestamp'], errors='coerce').dt.date
    shopee_details_chat['Order_Creation_Date'] = pd.to_datetime(shopee_details_chat['Order_Creation_Date'], errors='coerce').dt.date
    # Apply the custom function to calculate minutes
    shopee_details_chat['Reply_Minutes'] = shopee_details_chat['Response_Time'].apply(calculate_minutes)
    # Check how many entries were not convertible (resulted in None)
    non_convertible_count = shopee_details_chat['Reply_Minutes'].isna().sum()
    if non_convertible_count > 0:
        print(f"There are {non_convertible_count} entries with incorrect time format.")

    # Define stopwords
    stopwords = {"yet","hello","assist","today","has","which","now","tell","from","try","ya","1","2","may","let","my","so","have","your","do","don't","me","ask","us","our","take","really","about","waiting","soon","yes","please","how","what","where","when","can","hi","sorry","i","we","help","you","shopee","chat","content","type","not","supported","check","message","a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"}

    # Function to clean text by removing punctuation and stopwords
    def clean_text(text):
        import string
        text = text.lower().translate(str.maketrans('', '', string.punctuation))
        return ' '.join(word for word in text.split() if word not in stopwords)

    # Apply custom function to remove stopwords from 'Responded_Message'
    shopee_details_chat['Responded_Message'] = shopee_details_chat['Responded_Message'].apply(clean_text)

    # Custom column calculations
    shopee_details_chat['Time_Reply'] = np.select(
        [
            shopee_details_chat['Reply_Minutes'] < 60,
            shopee_details_chat['Reply_Minutes'].between(60, 119)
        ],
        [
            'Less than 1 hour',
            '1-2 hours'
        ],
        default='More than 2 hours'
    )

    shopee_details_chat['Order_Response_Check'] = np.select(
        [
            shopee_details_chat['Order_Creation_Date'].isna(),
            shopee_details_chat['Order_Creation_Date'] >= shopee_details_chat['Responded_Timestamp']
        ],
        [
            "Haven't ordered",
            "Order After Chat"
        ],
        default='Order Before Chat'
    )

    # Determine if the response is within office hours
    office_start = time(10, 0)  # Office starts at 10:00
    office_end = time(19, 0)    # Office ends at 19:00
    shopee_details_chat['Office_Hour'] = shopee_details_chat['Responded_Timestamp_Time'].apply(
        lambda x: 'Office Hours' if pd.notna(x) and office_start <= x <= office_end else 'Outside of Working Hours'
    )

    shopee_details_chat['Reply_5minutes'] = np.select(
        [
            shopee_details_chat['Reply_Minutes'] < 5,
        ],
        [
            'Within 5 minutes'
        ],
        default='More than 5 minutes')
    shopee_details_chat = shopee_details_chat.astype({
        'Sender':'str',
        'Responded_Timestamp':'str',
        'Response_Time':'str',
        'Responded_Message':'str',
        'Order_ID':'str',
        'Order_Status':'str',
        'Order_Creation_Date':'str',
        'Product_Name':'str',
        'SKU':'str',
        'Quantity':'Int64',
        'Total_Amount':'float64',
        'No_of_product_in_order':'Int64',
        'Responded_Timestamp_Time':'str',
        'Reply_Minutes':'float64',
        'Time_Reply':'str',
        'Order_Response_Check':'str',
        'Office_Hour':'str',
        'Reply_5minutes':'str'
    })
    shopee_details_chat.drop(columns='Response_Time', inplace=True)
    shopee_details_chat = shopee_details_chat.replace({np.nan: None, 'nan': None})
    # Explicitly convert NaT to None in the datetime column
    shopee_details_chat['Order_Creation_Date'] = shopee_details_chat['Order_Creation_Date'].apply(lambda x: None if pd.isna(x) else x)
    shopee_details_chat = shopee_details_chat.replace('NaT', None)

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = shopee_details_chat.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Details_Chat_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Mall_Order(import_data_path, export_data_path, time_suffix):
    laz_mall_ord = pd.read_excel(f"{import_data_path}\Laz_Mall_Order_{time_suffix}.xlsx")

    # Tính toán Quantity cho từng nhóm Order-SKU
    quantity_df = laz_mall_ord.groupby(['orderNumber', 'sellerSku']).size().reset_index(name='Quantity')

    # Right join df order và df quantity
    laz_mall_ord = pd.merge(laz_mall_ord, quantity_df, how='right', on=['orderNumber', 'sellerSku'])

    # Chuyển column `createTime` thành 2 columns chứa dữ liệu của Date và Time
    laz_mall_ord['CreationDate'] = pd.to_datetime(laz_mall_ord['createTime']).dt.date
    laz_mall_ord['CreationTime'] = pd.to_datetime(laz_mall_ord['createTime']).dt.strftime('%H:%M:%S')

    # Chỉ giữ lại 1 row cho 1 cặp Order-SKU
    laz_mall_ord['rn'] = laz_mall_ord.sort_values('createTime').groupby(['orderNumber', 'sellerSku']).cumcount() + 1
    result_laz_mall_ord = laz_mall_ord[laz_mall_ord['rn'] == 1]

    # Đổi tên cho những cột sử dụng
    result_laz_mall_ord = result_laz_mall_ord[[
        'orderNumber', 'sellerSku', 'deliveryType', 'CreationDate', 'CreationTime', 
        'customerName', 'payMethod', 'paidPrice', 'unitPrice', 'sellerDiscountTotal', 
        'shippingFee', 'itemName', 'status', 'buyerFailedDeliveryReturnInitiator', 'buyerFailedDeliveryReason', 'Quantity'
    ]].rename(columns={
        'orderNumber': 'Order_ID',
        'sellerSku': 'SKU',
        'deliveryType': 'Delivery_Type',
        'CreationDate': 'Creation_Date', 
        'CreationTime': 'Creation_Time',
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
    result_laz_mall_ord = result_laz_mall_ord.astype({
        'Order_ID': 'str',
        'SKU': 'str',
        'Delivery_Type': 'str',
        'Creation_Date': 'str',
        'Creation_Time': 'str',
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

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_mall_ord.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Mall_Order_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Mall_Key_Metrics(import_data_path, export_data_path, time_suffix):
    laz_mall_key_metrics = pd.read_excel(f"{import_data_path}\Laz_Mall_Key_Metrics_{time_suffix}.xls", header = 5, skiprows = [6,7], na_values='-')
    
    # Chuyển số liệu của cột 'Conversion Rate' sang dạng float
    laz_mall_key_metrics['Conversion Rate'] = laz_mall_key_metrics['Conversion Rate'].apply(convert_percentage)
    
    # Đổi tên các cột cần thiết
    result_laz_mall_key_metrics = laz_mall_key_metrics.rename(columns={
        "Pageviews": "Page_views", 
        "Units Sold": "Units_Sold",
        "Conversion Rate": "Conversion_Rate", 
        "Revenue per Buyer": "Revenue_per_Buyer", 
        "Visitor Value": "Visitor_Value", 
        "Add to Cart Users": "Add_to_Cart_Users", 
        "Add to Cart Units": "Add_to_Cart_Units", 
        "Wishlist Users": "Wishlist_Users", 
        "Average Order Value": "Average_Order_Value", 
        "Average Basket Size": "Average_Basket_Size", 
        "Cancelled Amount": "Cancelled_Amount", 
        "Return/Refund Amount": "Return_Refund_Amount"})
    
    # Chỉnh lại datatypes tương ứng
    result_laz_mall_key_metrics = result_laz_mall_key_metrics.astype({
        "Date": "str",
        "Revenue": "float64",
        "Visitors": "int64",
        "Buyers": "int64",
        "Orders": "int64",
        "Page_views": "int64",
        "Units_Sold": "int64",
        "Conversion_Rate": "float64",
        "Revenue_per_Buyer": "float64",
        "Visitor_Value": "float64",
        "Add_to_Cart_Users": "int64",
        "Add_to_Cart_Units": "int64",
        "Wishlists": "int64",
        "Wishlist_Users": "int64",
        "Average_Order_Value": "float64",
        "Average_Basket_Size": "float64",
        "Cancelled_Amount": "float64",
        "Return_Refund_Amount": "float64"})
    
    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_mall_key_metrics.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Mall_Key_Metrics_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Mall_Return_Refund(import_data_path, export_data_path, time_suffix):
    try:
        list = [pd.read_excel(filename) for filename in glob.glob(f"{import_data_path}\Laz_Mall_Return_Refund_{time_suffix}*.xlsx")]
        laz_mall_return_refund = pd.concat(list, axis=0)
    except:
        laz_mall_return_refund = pd.read_excel(f"{import_data_path}\Laz_Mall_Return_Refund_{time_suffix}.xlsx")    

    # Format lại những cột ngày để tránh bị lỗi khi load into bigquery
    laz_mall_return_refund['Order Date'] = pd.to_datetime(laz_mall_return_refund['Order Date']).dt.date
    laz_mall_return_refund['Return Order Date'] = pd.to_datetime(laz_mall_return_refund['Return Order Date']).dt.date

    # Đổi tên các cột cần thiết
    result_laz_mall_return_refund = laz_mall_return_refund[[
        'Order ID', 'Order Date', 'Return Order Date', 'Seller SKU ID', 'Item Name', 
        'Paid Price + Shipping Fee', 'Refund Amount', 'Return Reason', 'Status'
    ]].rename(columns={
        'Order ID': 'Order_ID', 
        'Order Date': 'Order_Date', 
        'Return Order Date': 'Return_Order_Date', 
        'Seller SKU ID': 'Seller_SKU_ID', 
        'Item Name': 'Item_Name', 
        'Paid Price + Shipping Fee': 'Paid_Price_Shipping_Fee', 
        'Refund Amount': 'Refund_Amount', 
        'Return Reason': 'Return_Reason'
    })

    # Chỉnh lại datatypes tương ứng
    result_laz_mall_return_refund = result_laz_mall_return_refund.astype({
        'Order_ID':'str', 
        'Order_Date':'str', 
        'Return_Order_Date':'str', 
        'Seller_SKU_ID':'str', 
        'Item_Name':'str', 
        'Paid_Price_Shipping_Fee':'float64', 
        'Refund_Amount':'float64', 
        'Return_Reason':'str'
    })

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_mall_return_refund.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Mall_Return_Refund_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Mall_Product(import_data_path, export_data_path, time_suffix):
    laz_mall_product = pd.read_excel(f"{import_data_path}\Laz_Mall_Product_{time_suffix}.xls", header = 5)

    # Filter rows where 'Seller SKU' is not "-"
    #laz_mall_product = laz_mall_product[laz_mall_product['Seller SKU'] != "-"]

    # Chuyển các bản ghi "-" thành 0
    laz_mall_product['Revenue per Buyer'] = laz_mall_product['Revenue per Buyer'].replace("-",0)
    laz_mall_product['Revenue share'] = laz_mall_product['Revenue share'].replace("-",0).apply(convert_percentage)

    # Remove những Columns không sử dụng
    laz_mall_product = laz_mall_product.drop(columns=["Product Visitors", "Product Pageviews", "Visitor Value", "Add to Cart Conversion Rate", "Conversion Rate", "URL"])

    # Rename columns
    result_laz_mall_product = laz_mall_product.rename(columns={
        "Product Name": "Product_Name",
        "Product ID": "Product_ID",
        "Seller SKU": "Seller_SKU",
        "SKU ID": "SKU_ID",
        "Add to Cart Users": "Add_to_Cart_Users",
        "Add to Cart Units": "Add_to_Cart_Units",
        "Wishlist Users": "Wishlist_Users",
        "Units Sold": "Units_Sold",
        "Revenue per Buyer": "Revenue_per_Buyer",
        "Revenue share": "Revenue_share"
    })

    # Thêm cột date cho file Product của từng tháng
    result_laz_mall_product['Date'] = get_first_date_of_month(time_suffix)

    # Change datatypes
    result_laz_mall_product = result_laz_mall_product.astype({
        "Product_Name":"str",
        "Product_ID":"str",
        "Seller_SKU":"str",
        "SKU_ID":"str",
        "Add_to_Cart_Users":"int64",
        "Add_to_Cart_Units":"int64",
        "Wishlist_Users":"int64",
        "Wishlists":"int64",
        "Buyers":"int64",
        "Orders":"int64",
        "Units_Sold":"int64",
        "Revenue":"float64",
        "Revenue_per_Buyer":"float64",
        "Revenue_share":"float64",
        "Date":"str"
    })

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_mall_product.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Mall_Product_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Order(import_data_path, export_data_path, time_suffix):
    laz_ord = pd.read_excel(f"{import_data_path}\Laz_Order_{time_suffix}.xlsx")

    # Tính toán Quantity cho từng nhóm Order-SKU
    quantity_df = laz_ord.groupby(['orderNumber', 'sellerSku']).size().reset_index(name='Quantity')

    # Right join df order và df quantity
    laz_ord = pd.merge(laz_ord, quantity_df, how='right', on=['orderNumber', 'sellerSku'])

    # Chuyển column `createTime` thành 2 columns chứa dữ liệu của Date và Time
    laz_ord['CreationDate'] = pd.to_datetime(laz_ord['createTime']).dt.date
    laz_ord['CreationTime'] = pd.to_datetime(laz_ord['createTime']).dt.strftime('%H:%M:%S')

    # Chỉ giữ lại 1 row cho 1 cặp Order-SKU
    laz_ord['rn'] = laz_ord.sort_values('createTime').groupby(['orderNumber', 'sellerSku']).cumcount() + 1
    result_laz_ord = laz_ord[laz_ord['rn'] == 1]

    # Đổi tên cho những cột sử dụng
    result_laz_ord = result_laz_ord[[
        'orderNumber', 'sellerSku', 'deliveryType', 'CreationDate', 'CreationTime', 
        'customerName', 'payMethod', 'paidPrice', 'unitPrice', 'sellerDiscountTotal', 
        'shippingFee', 'itemName', 'status', 'buyerFailedDeliveryReturnInitiator', 'buyerFailedDeliveryReason', 'Quantity'
    ]].rename(columns={
        'orderNumber': 'Order_ID',
        'sellerSku': 'SKU',
        'deliveryType': 'Delivery_Type',
        'CreationDate': 'Creation_Date', 
        'CreationTime': 'Creation_Time',
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
    result_laz_ord = result_laz_ord.astype({
        'Order_ID': 'str',
        'SKU': 'str',
        'Delivery_Type': 'str',
        'Creation_Date': 'str',
        'Creation_Time': 'str',
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

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_ord.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Order_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Key_Metrics(import_data_path, export_data_path, time_suffix):
    laz_key_metrics = pd.read_excel(f"{import_data_path}\Laz_Key_Metrics_{time_suffix}.xls", header = 5, skiprows = [6,7], na_values='-')
    
    # Chuyển số liệu của cột 'Conversion Rate' sang dạng float
    laz_key_metrics['Conversion Rate'] = laz_key_metrics['Conversion Rate'].apply(convert_percentage)
    
    # Đổi tên các cột cần thiết
    result_laz_key_metrics = laz_key_metrics.rename(columns={
        "Pageviews": "Page_views", 
        "Units Sold": "Units_Sold",
        "Conversion Rate": "Conversion_Rate", 
        "Revenue per Buyer": "Revenue_per_Buyer", 
        "Visitor Value": "Visitor_Value", 
        "Add to Cart Users": "Add_to_Cart_Users", 
        "Add to Cart Units": "Add_to_Cart_Units", 
        "Wishlist Users": "Wishlist_Users", 
        "Average Order Value": "Average_Order_Value", 
        "Average Basket Size": "Average_Basket_Size", 
        "Cancelled Amount": "Cancelled_Amount", 
        "Return/Refund Amount": "Return_Refund_Amount"})
    
    # Chỉnh lại datatypes tương ứng
    result_laz_key_metrics = result_laz_key_metrics.astype({
        "Date": "str",
        "Revenue": "float64",
        "Visitors": "int64",
        "Buyers": "int64",
        "Orders": "int64",
        "Page_views": "int64",
        "Units_Sold": "int64",
        "Conversion_Rate": "float64",
        "Revenue_per_Buyer": "float64",
        "Visitor_Value": "float64",
        "Add_to_Cart_Users": "int64",
        "Add_to_Cart_Units": "int64",
        "Wishlists": "int64",
        "Wishlist_Users": "int64",
        "Average_Order_Value": "float64",
        "Average_Basket_Size": "float64",
        "Cancelled_Amount": "float64",
        "Return_Refund_Amount": "float64"})

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_key_metrics.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Key_Metrics_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Return_Refund(import_data_path, export_data_path, time_suffix):
    try:
        list = [pd.read_excel(filename) for filename in glob.glob(f"{import_data_path}\Laz_Return_Refund_{time_suffix}*.xlsx")]
        laz_return_refund = pd.concat(list, axis=0)
    except:
        laz_return_refund = pd.read_excel(f"{import_data_path}\Laz_Return_Refund_{time_suffix}.xlsx")

    # Format lại những cột ngày để tránh bị lỗi khi load into bigquery
    laz_return_refund['Order Date'] = pd.to_datetime(laz_return_refund['Order Date']).dt.date
    laz_return_refund['Return Order Date'] = pd.to_datetime(laz_return_refund['Return Order Date']).dt.date

    # Đổi tên các cột cần thiết
    result_laz_return_refund = laz_return_refund[[
        'Order ID', 'Order Date', 'Return Order Date', 'Seller SKU ID', 'Item Name', 
        'Paid Price + Shipping Fee', 'Refund Amount', 'Return Reason', 'Status'
    ]].rename(columns={
        'Order ID': 'Order_ID', 
        'Order Date': 'Order_Date', 
        'Return Order Date': 'Return_Order_Date', 
        'Seller SKU ID': 'Seller_SKU_ID', 
        'Item Name': 'Item_Name', 
        'Paid Price + Shipping Fee': 'Paid_Price_Shipping_Fee', 
        'Refund Amount': 'Refund_Amount', 
        'Return Reason': 'Return_Reason'
    })

    # Chỉnh lại datatypes tương ứng
    result_laz_return_refund = result_laz_return_refund.astype({
        'Order_ID':'str', 
        'Order_Date':'str', 
        'Return_Order_Date':'str', 
        'Seller_SKU_ID':'str', 
        'Item_Name':'str', 
        'Paid_Price_Shipping_Fee':'float64', 
        'Refund_Amount':'float64', 
        'Return_Reason':'str'
    })

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_return_refund.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Return_Refund_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Product(import_data_path, export_data_path, time_suffix):
    laz_product = pd.read_excel(f"{import_data_path}\Laz_Product_{time_suffix}.xls", header = 5)

    # Filter rows where 'Seller SKU' is not "-"
    #laz_product = laz_product[laz_product['Seller SKU'] != "-"]

    # Chuyển các bản ghi "-" thành 0
    laz_product['Revenue per Buyer'] = laz_product['Revenue per Buyer'].replace("-",0)
    laz_product['Revenue share'] = laz_product['Revenue share'].replace("-",0).apply(convert_percentage)

    # Remove những Columns không sử dụng
    laz_product = laz_product.drop(columns=["Product Visitors", "Product Pageviews", "Visitor Value", "Add to Cart Conversion Rate", "Conversion Rate", "URL"])

    # Rename columns
    result_laz_product = laz_product.rename(columns={
        "Product Name": "Product_Name",
        "Product ID": "Product_ID",
        "Seller SKU": "Seller_SKU",
        "SKU ID": "SKU_ID",
        "Add to Cart Users": "Add_to_Cart_Users",
        "Add to Cart Units": "Add_to_Cart_Units",
        "Wishlist Users": "Wishlist_Users",
        "Units Sold": "Units_Sold",
        "Revenue per Buyer": "Revenue_per_Buyer",
        "Revenue share": "Revenue_share"
    })

    # Thêm cột date cho file Product của từng tháng
    result_laz_product['Date'] = get_first_date_of_month(time_suffix)

    # Change datatypes
    result_laz_product = result_laz_product.astype({
        "Product_Name":"str",
        "Product_ID":"str",
        "Seller_SKU":"str",
        "SKU_ID":"str",
        "Add_to_Cart_Users":"int64",
        "Add_to_Cart_Units":"int64",
        "Wishlist_Users":"int64",
        "Wishlists":"int64",
        "Buyers":"int64",
        "Orders":"int64",
        "Units_Sold":"int64",
        "Revenue":"float64",
        "Revenue_per_Buyer":"float64",
        "Revenue_share":"float64",
        "Date":"str"
    })

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = result_laz_product.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Laz_Product_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Laz_Mall_Chat(import_data_path, time_suffix):
    laz_mall_chat = pd.read_excel(f"{import_data_path}\Laz_Mall_Chat_{time_suffix}.xls", header = 5, skiprows = [6,7])
    # Replace '-' with specified values
    replacements = {
        "Response Rate (Holiday Mode)": 1,
        "Enquiry Rate": 0,
        "Received Conversations": 0,
        "Responded Conversations": 0,
        "Average Response Time": 0,
        "Customers Enquired": 0,
        "Response Rate": 1,
        "Responded Customers":0,
        "Guided Buyers": 0,
        "Guided Revenue": 0,
        "Conversion Rate": 0,
        "Guided Orders": 0,
        "Estimated Lost Revenue": 0,
        "Non-Responded Customers": 0,
    }

    # Apply replacements for each '-' entry in the DataFrame
    for column, replacement in replacements.items():
        laz_mall_chat[column] = laz_mall_chat[column].replace('-', replacement)

    # Format lại các cột percentage
    laz_mall_chat['Response Rate (Holiday Mode)'] = laz_mall_chat['Response Rate (Holiday Mode)'].apply(convert_percentage)
    laz_mall_chat['Enquiry Rate'] = laz_mall_chat['Enquiry Rate'].apply(convert_percentage)
    laz_mall_chat['Response Rate'] = laz_mall_chat['Response Rate'].apply(convert_percentage)
    laz_mall_chat['Conversion Rate'] = laz_mall_chat['Conversion Rate'].apply(convert_percentage)

    # Rename columns
    laz_mall_chat.rename(columns=lambda x: x.replace(' ', '_').replace('(', '').replace(')', '').replace('-','_'), inplace=True)

    # Convert column types
    laz_mall_chat = laz_mall_chat.astype({
        "Date": 'str',
        "Visitors": 'int64',
        "Response_Rate_Holiday_Mode": 'float64',
        "Enquiry_Rate": 'float64',
        "Received_Conversations": 'int64',
        "Responded_Conversations": 'int64',
        "Average_Response_Time": 'float64',
        "Customers_Enquired": 'int64',
        "Response_Rate": 'float64',
        "Responded_Customers": 'int64',
        "Guided_Buyers": 'int64',
        "Guided_Revenue": 'float64',
        "Conversion_Rate": 'float64',
        "Guided_Orders": 'int64',
        "Non_Responded_Customers": 'int64',
        "Estimated_Lost_Revenue": 'float64',
    }, errors='ignore') 

    # Add Time_Reply column
    laz_mall_chat['Time_Reply'] = laz_mall_chat['Average_Response_Time'].apply(classify_response_time)

    # Thêm Source column
    laz_mall_chat['Source'] = "Lazada Mall"
    
    return laz_mall_chat

def Laz_Chat(import_data_path, time_suffix):
    laz_chat = pd.read_excel(f"{import_data_path}\Laz_Chat_{time_suffix}.xls", header = 5, skiprows = [6,7])
    # Replace '-' with specified values
    replacements = {
        "Response Rate (Holiday Mode)": 1,
        "Enquiry Rate": 0,
        "Received Conversations": 0,
        "Responded Conversations": 0,
        "Average Response Time": 0,
        "Customers Enquired": 0,
        "Response Rate": 1,
        "Responded Customers":0,
        "Guided Buyers": 0,
        "Guided Revenue": 0,
        "Conversion Rate": 0,
        "Guided Orders": 0,
        "Estimated Lost Revenue": 0,
        "Non-Responded Customers": 0,
    }

    # Apply replacements for each '-' entry in the DataFrame
    for column, replacement in replacements.items():
        laz_chat[column] = laz_chat[column].replace('-', replacement)

    # Format lại các cột percentage
    laz_chat['Response Rate (Holiday Mode)'] = laz_chat['Response Rate (Holiday Mode)'].apply(convert_percentage)
    laz_chat['Enquiry Rate'] = laz_chat['Enquiry Rate'].apply(convert_percentage)
    laz_chat['Response Rate'] = laz_chat['Response Rate'].apply(convert_percentage)
    laz_chat['Conversion Rate'] = laz_chat['Conversion Rate'].apply(convert_percentage)

    # Rename columns
    laz_chat.rename(columns=lambda x: x.replace(' ', '_').replace('(', '').replace(')', '').replace('-','_'), inplace=True)

    # Convert column types
    laz_chat = laz_chat.astype({
        "Date": 'str',
        "Visitors": 'int64',
        "Response_Rate_Holiday_Mode": 'float64',
        "Enquiry_Rate": 'float64',
        "Received_Conversations": 'int64',
        "Responded_Conversations": 'int64',
        "Average_Response_Time": 'float64',
        "Customers_Enquired": 'int64',
        "Response_Rate": 'float64',
        "Responded_Customers": 'int64',
        "Guided_Buyers": 'int64',
        "Guided_Revenue": 'float64',
        "Conversion_Rate": 'float64',
        "Guided_Orders": 'int64',
        "Non_Responded_Customers": 'int64',
        "Estimated_Lost_Revenue": 'float64',
    }, errors='ignore') 

    # Add Time_Reply column
    laz_chat['Time_Reply'] = laz_chat['Average_Response_Time'].apply(classify_response_time)

    # Thêm Source column
    laz_chat['Source'] = "Lazada"

    return laz_chat

def Combined_Laz_Chat(import_data_path, export_data_path, time_suffix):
    try:
        laz_mall_chat = Laz_Mall_Chat(import_data_path, time_suffix)
        laz_chat = Laz_Chat(import_data_path, time_suffix)

        # Concatenate laz_mall_chat and laz_chat vertically
        combined_laz_chat = pd.concat([laz_mall_chat, laz_chat], axis=0)
    except Exception as e:
        print(f"An error occurred during concatenation: {e}. Attempting fallback.")
        try:
            combined_laz_chat = Laz_Mall_Chat(import_data_path, time_suffix)
        except Exception as e_mall:
            print(f"Failed to load Laz_Mall_Chat: {e_mall}. Attempting to load Laz_Chat instead.")
            try:
                combined_laz_chat = Laz_Chat(import_data_path, time_suffix)
            except Exception as e_chat:
                print(f"Failed to load Laz_Chat: {e_chat}. No data available.")

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = combined_laz_chat.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Combined_Laz_Chat_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)

def Shopee_Product(import_data_path, export_data_path, time_suffix):
    df = pd.read_excel(f"{import_data_path}\Shopee_Product_{time_suffix}.xlsx", na_values='-', thousands=",")
    
    # # Filter rows where 'Seller SKU' is not "-"
    # df.dropna(subset=['SKU'], inplace = True)

    # Xóa những dòng có Current_Variation_Status == "Deleted"
    df = df[df['Current Variation Status'] != "Deleted"]

    # fillna cột 'SKU' bằng giá trị ở cột 'Parent SKU'
    df['SKU'] = df['SKU'].fillna(df['Parent SKU'])

    # Remove những Columns không sử dụng
    df = df.drop(columns=["Parent SKU"])

    # Chuyển những cột ở dạng % sang float
    for column in ["Product Bounce Rate", "Conversion Rate (Add to Cart)", "Conversion Rate (Placed Order)", "Conversion Rate (Paid Order)", "Repeat Order Rate (Paid Order)"]:
        df[column] = df[column].apply(convert_percentage)

    # Đổi tên các cột cần thiết
    df.rename(columns={
        "Item ID": "Item_ID",
        "Current Item Status": "Current_Item_Status",
        "Variation ID": "Variation_ID",
        "Variation Name": "Variation_Name",
        "Current Variation Status": "Current_Variation_Status",
        "Product Visitors (Visit)": "Product_Visitors_Visit",
        "Product Page Views": "Product_Page_Views",
        "Product Bounce Visitors": "Product_Bounce_Visitors",
        "Product Bounce Rate": "Product_Bounce_Rate",
        "Search Clicks": "Search_Clicks",
        "Product Visitors (Add to Cart)": "Product_Visitors_Add_to_Cart",
        "Units (Add to Cart)": "Units_Add_to_Cart",
        "Conversion Rate (Add to Cart)": "Conversion_Rate_Add_to_Cart",
        "Buyers (Placed Order)": "Buyers_Placed_Order",
        "Units (Placed Order)": "Units_Placed_Order",
        "Sales (Placed Order) (SGD)": "Sales_Placed_Order",
        "Conversion Rate (Placed Order)": "Conversion_Rate_Placed_Order",
        "Buyers (Paid Order)": "Buyers_Paid_Order",
        "Units (Paid Order)": "Units_Paid_Order",
        "Sales (Paid Order) (SGD)": "Sales_Paid_Order",
        "Conversion Rate (Paid Order)": "Conversion_Rate_Paid_Order",
        "Repeat Order Rate (Paid Order)": "Repeat_Order_Rate_Paid_Order",
        "Average Days to Repeat Order (Paid Order)": "Average_Days_to_Repeat_Order_Paid_Order"
    }, inplace=True)

    # Thêm cột date cho file Product của từng tháng
    df['Date'] = get_first_date_of_month(time_suffix)

    # Convert column datatypes
    df = df.astype({
        "Item_ID": 'Int64',
        "Variation_ID": 'Int64',
        "Product_Visitors_Visit": 'Int64',
        "Product_Page_Views": 'Int64',
        "Product_Bounce_Visitors": 'Int64',
        "Product_Bounce_Rate": 'float64',
        "Search_Clicks": 'Int64',
        "Likes": 'Int64',
        "Product_Visitors_Add_to_Cart": 'Int64',
        "Units_Add_to_Cart": 'Int64',
        "Conversion_Rate_Add_to_Cart": 'float64',
        "Buyers_Placed_Order": 'Int64',
        "Units_Placed_Order": 'Int64',
        "Sales_Placed_Order": 'float64',
        "Conversion_Rate_Placed_Order": 'float64',
        "Buyers_Paid_Order": 'Int64',
        "Units_Paid_Order": 'Int64',
        "Sales_Paid_Order": 'float64',
        "Conversion_Rate_Paid_Order": 'float64',
        "Repeat_Order_Rate_Paid_Order": 'float64',
        "Average_Days_to_Repeat_Order_Paid_Order": 'float64',
    })

    # Xử lý lỗi liên quan tới escaping slashes khi xuất file sang định dạng json
    df_without_escaping_slashes = df.to_json(orient='records', lines=True)
    df_without_escaping_slashes = df_without_escaping_slashes.replace('\\/', '/')

    # Xuất file sang định dạng json
    with open(f"{export_data_path}\Shopee_Product_{time_suffix}.json", 'w', encoding='utf-8') as file:
        file.write(df_without_escaping_slashes)


def TikTok_Linked_Account(gcs_data_path):
  # Read only the first row of the Excel file
  first_row_df = pd.read_excel(gcs_data_path, header=None, nrows=1)

  # Extract the value from the first row
  first_row_value = first_row_df.iloc[0, 0].split(' ~ ')[1].strip('\n')

  df = pd.read_excel(gcs_data_path, header=2, na_values='--')
  for column in ['CTR', 'V-to-L rate', 'Video Finish Rate', 'CTOR']:
    df[column] = df[column].apply(convert_percentage)  

  df['Created_Date'] = pd.to_datetime(df['Time']).dt.date
  df.drop(columns=['Video Info','Time'], inplace = True)
  df.rename(columns=
              {'Creator name':'Creator_Name',
               'Creator ID':'Creator_ID',
               'Video ID':'Video_ID',
               'VV':'Views',
               'New followers':'New_Followers',
               'V-to-L clicks':'VtoL_Clicks',
               'Product Impressions':'Product_Impressions',
               'Product Clicks':'Product_Clicks',
               'Unit Sales':'Unit_Sales',
               'Video Revenue (S$)':'Video_Revenue',
               'GPM (S$)':'GPM',
               'Shoppable video attributed GMV (S$)':'GMV',
               'V-to-L rate':'VtoL_Rate',
               'Video Finish Rate':'Video_Finish_Rate'}, inplace = True)
  df = df.astype({
    'Creator_Name':'str', 
    'Creator_ID':'str', 
    'Video_ID':'str', 
    'Products':'str', 
    'Views':'int64',
    'Likes':'int64', 
    'Comments':'int64', 
    'Shares':'int64', 
    'New_Followers':'int64', 
    'VtoL_Clicks':'int64',
    'Product_Impressions':'int64', 
    'Product_Clicks':'int64', 
    'Buyers':'int64', 
    'Orders':'int64',
    'Unit_Sales':'int64', 
    'Video_Revenue':'float64', 
    'GPM':'float64', 
    'GMV':'float64', 
    'CTR':'float64', 
    'VtoL_Rate':'float64',
    'Video_Finish_Rate':'float64',
    'CTOR':'float64'
  })

  # Tạo cột Recorded_Date thể hiện ngày cuối cùng của trong data tải về của tuần đó
  df['Recorded_Date'] = first_row_value
  df['Recorded_Date'] = pd.to_datetime(df['Recorded_Date'], format='%Y-%m-%d').dt.date

  return df

def TikTok_Affiliate_Account(gcs_data_path):
  # Read only the first row of the Excel file
  first_row_df = pd.read_excel(gcs_data_path, header=None, nrows=1)

  # Extract the value from the first row
  first_row_value = first_row_df.iloc[0, 0].split(' ~ ')[1].strip('\n')

  df = pd.read_excel(gcs_data_path, header=2, na_values='--')
  for column in ['CTR', 'V-to-L rate', 'Video Finish Rate', 'CTOR']:
    df[column] = df[column].apply(convert_percentage)  

  df['Created_Date'] = pd.to_datetime(df['Time']).dt.date
  df.drop(columns=['Video Info','Time'], inplace = True)
  df.rename(columns=
              {'Creator name':'Creator_Name',
               'Creator ID':'Creator_ID',
               'Video ID':'Video_ID',
               'VV':'Views',
               'New followers':'New_Followers',
               'V-to-L clicks':'VtoL_Clicks',
               'Product Impressions':'Product_Impressions',
               'Product Clicks':'Product_Clicks',
               'Unit Sales':'Unit_Sales',
               'Video Revenue (S$)':'Video_Revenue',
               'GPM (S$)':'GPM',
               'Shoppable video attributed GMV (S$)':'GMV',
               'V-to-L rate':'VtoL_Rate',
               'Video Finish Rate':'Video_Finish_Rate'}, inplace = True)
  df = df.astype({
    'Creator_Name':'str', 
    'Creator_ID':'str', 
    'Video_ID':'str',  
    'Products':'str', 
    'Views':'int64',
    'Likes':'int64', 
    'Comments':'int64', 
    'Shares':'int64', 
    'New_Followers':'int64', 
    'VtoL_Clicks':'int64',
    'Product_Impressions':'int64', 
    'Product_Clicks':'int64', 
    'Buyers':'int64', 
    'Orders':'int64',
    'Unit_Sales':'int64', 
    'Video_Revenue':'float64', 
    'GPM':'float64', 
    'GMV':'float64', 
    'CTR':'float64', 
    'VtoL_Rate':'float64',
    'Video_Finish_Rate':'float64',
    'CTOR':'float64'
  })

  # Tạo cột Recorded_Date thể hiện ngày cuối cùng của trong data tải về của tuần đó
  df['Recorded_Date'] = first_row_value
  df['Recorded_Date'] = pd.to_datetime(df['Recorded_Date'], format='%Y-%m-%d').dt.date

  return df

def TikTok_Product_Video(gcs_data_path):
  # Read only the first row of the Excel file
  first_row_df = pd.read_excel(gcs_data_path, header=None, nrows=1)

  # Extract the value from the first row
  first_row_value = first_row_df.iloc[0, 0].split(' ~ ')[1].strip('\n')

  df = pd.read_excel(gcs_data_path, header=2, na_values='--')
  df = df[['ID', 'Product', 'GMV', 'Units sold', 'Orders', 'Video GMV', 'Video units sold', 'Video impressions', 'Page views from video','Video unique product buyers',
       'Video click-through rate', 'Video conversion rate']]
  for column in ['Video click-through rate', 'Video conversion rate']:
    df[column] = df[column].apply(convert_percentage) 
  for column in ['Video GMV', 'GMV']:
    df[column] = df[column].apply(clean_currency_string) 
  df.rename(columns=
          {'Units sold':'Units_Sold',
           'Video GMV':'Video_GMV',
           'Video units sold':'Video_Units_Sold', 
           'Video impressions':'Video_Impressions', 
           'Page views from video':'Page_Views_From_Video',
           'Video unique product buyers':'Video_Unique_Product_Buyers',
           'Video click-through rate':'Video_Clickthrough_Rate',
           'Video conversion rate':'Video_Conversion_Rate'}, inplace=True)
  df = df.astype({'ID':'str', 
                'Product':'str', 
                'GMV':'float64', 
                'Units_Sold':'int64', 
                'Orders':'int64', 
                'Video_GMV':'float64',
                'Video_Units_Sold':'int64', 
                'Video_Impressions':'int64', 
                'Page_Views_From_Video':'int64',
                'Video_Unique_Product_Buyers':'int64', 
                'Video_Clickthrough_Rate':'float64',
                'Video_Conversion_Rate':'float64'})
  # Tạo cột Recorded_Date thể hiện ngày cuối cùng của trong data tải về của tuần đó
  df['Recorded_Date'] = first_row_value
  df['Recorded_Date'] = pd.to_datetime(df['Recorded_Date'], format='%Y-%m-%d').dt.date

  return df

def TikTok_Affiliate_Video(gcs_data_path):
    # Read only the first row of the Excel file
    first_row_df = pd.read_excel(gcs_data_path, header=None, nrows=1)

    # Extract the value from the first row
    first_row_value = first_row_df.iloc[0, 0].split(' ~ ')[1].strip('\n')

    df = pd.read_excel(gcs_data_path, header=2)

    def rename_column(col_name):
        # Remove spaces and dots, split into words by spaces
        words = col_name.replace('.', '').split()
        # Capitalize the first letter of each word and join with underscores
        result = '_'.join([word[0].upper() + word[1:] for word in words])
        return result

    # Apply the renaming function to all columns in the DataFrame
    df.columns = [rename_column(col) for col in df.columns]

    df['Affiliate_CTR'] = df['Affiliate_CTR'].apply(convert_percentage) 

    df['Created_Date'] = pd.to_datetime(df['Video_Post_Date']).dt.date
    df.drop(columns=['Video_Post_Date'], inplace = True)

    df = df.astype({
        'Video_Name': 'str',
        'Creator_Username': 'str',
        'Affiliate_CTR': 'float64',
        'Shoppable_Video_GPM': 'float64',
        'Affiliate_Shoppable_Video_GMV': 'float64',
        'Affiliate_Items_Sold': 'int64',
        'Shoppable_Video_Impressions': 'int64',
        'Shoppable_Video_Likes': 'int64',
        'Shoppable_Video_Comments': 'int64',
        'GMV': 'float64',
        'Affiliate_Orders': 'int64',
        'Affiliate_Items_Refunded': 'int64',
        'Affiliate_Refunded_GMV': 'float64',
        'Est_Commission': 'float64',
        'Shoppable_Video_Avg_Order_Value': 'float64',
        'Avg_Affiliate_Customers': 'float64'
    })

    df.fillna(0, inplace=True)

    # Tạo cột Recorded_Date thể hiện ngày cuối cùng của trong data tải về của tuần đó
    df['Recorded_Date'] = first_row_value
    df['Recorded_Date'] = pd.to_datetime(df['Recorded_Date'], format='%Y-%m-%d').dt.date

    return df