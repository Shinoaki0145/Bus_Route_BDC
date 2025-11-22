import pandas as pd
import glob
import os

root_folder = r'E:/BDC/Bus_route_data/HCMC_bus_routes'

all_stops_list = []


search_patterns = [
    os.path.join(root_folder, '*', 'stops_by_var.csv'),
    os.path.join(root_folder, '*', 'rev_stops_by_var.csv')
]

print("Đang tổng hợp dữ liệu các trạm...")

for pattern in search_patterns:
    for file_path in glob.glob(pattern):
        try:
            # Đọc file
            df = pd.read_csv(file_path)
            
            cols_to_keep = ['StopId', 'Code', 'Lat', 'Lng', 'Name', 'StopType']
            
            # Lọc chỉ lấy cột tồn tại trong file để tránh lỗi
            existing_cols = [c for c in cols_to_keep if c in df.columns]
            df = df[existing_cols]
            
            all_stops_list.append(df)
        except Exception as e:
            print(f"Lỗi đọc file {file_path}: {e}")


if all_stops_list:
    master_stops = pd.concat(all_stops_list, ignore_index=True)
    

    master_stops = master_stops.drop_duplicates(subset=['StopId'])
 
    master_stops.to_csv('master_unique_stops.csv', index=False)
    print(f"Đã tạo xong file 'master_unique_stops.csv' với {len(master_stops)} trạm duy nhất.")
else:
    print("Không tìm thấy file nào.")
    
    
    
    