from sklearn.neighbors import BallTree
import numpy as np
import pandas as pd


root_folder = r'E:/BDC'

df_stops = pd.read_csv(f'{root_folder}/master_unique_stops.csv')

df_raw = pd.read_csv(f'{root_folder}/processed_raw_GPS_all_days.csv')

print("Đang xử lý mapping...")

stops_rad = np.radians(df_stops[['Lat', 'Lng']].values)
raw_rad = np.radians(df_raw[['lat', 'lng']].values)

# 3. Xây dựng BallTree
tree = BallTree(stops_rad, metric='haversine')


distances, indices = tree.query(raw_rad, k=1)


distances_in_meters = distances * 6371000

df_raw['distance_diff_meters'] = distances_in_meters 

# Gán thông tin trạm tìm được
df_raw['StopId'] = df_stops.iloc[indices.flatten()]['StopId'].values
df_raw['StopType'] = df_stops.iloc[indices.flatten()]['StopType'].values

df_raw['StopLng'] = df_stops.iloc[indices.flatten()]['Lng'].values
df_raw['StopLat'] = df_stops.iloc[indices.flatten()]['Lat'].values


THRESHOLD_METERS = 100 # 100 mét
matched_data = df_raw[df_raw['distance_diff_meters'] <= THRESHOLD_METERS].copy()

matched_data = matched_data.drop(columns=['distance_diff_meters'], errors='ignore')


print("Hoàn tất!")
print(f"Đã lọc và chuẩn hóa {len(matched_data)} điểm.")


df_sorted = matched_data.sort_values(by=['anonymized_vehicle', 'time_minute'], ascending=[True, True])

# 4. Lưu kết quả ra file mới
df_sorted.to_csv('mapping_gps_to_stops.csv', index=False)

print("Đã sắp xếp xong và lưu vào file 'mapping_gps_to_stops.csv'")
# Hiển thị 5 dòng đầu để kiểm tra
print(df_sorted.head())