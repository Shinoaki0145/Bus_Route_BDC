import pandas as pd

# 1. Đọc dữ liệu
df = pd.read_csv('mapping_gps_to_stops.csv')

# 2. Tiền xử lý: Đảm bảo datetime đúng định dạng và đã sort (như bước trước)
df['time_minute'] = pd.to_datetime(df['time_minute'])
df = df.sort_values(by=['anonymized_vehicle', 'time_minute'])

# Danh sách để lưu kết quả các hành trình
all_trips = []

# 3. Xử lý logic tách hành trình cho từng xe
# Groupby theo xe để xử lý độc lập
for vehicle_id, vehicle_data in df.groupby('anonymized_vehicle'):
    
    # Các biến trạng thái
    is_on_trip = False          # Đang đi trên đường hay không
    trip_start_time = None      # Thời điểm bắt đầu (lấy từ dòng Bến xe cuối cùng)
    last_station_time = None    # Lưu lại thời điểm dòng 'Bến xe' gần nhất vừa đọc
    
    # Duyệt qua từng dòng dữ liệu của xe đó
    for index, row in vehicle_data.iterrows():
        curr_time = row['time_minute']
        stop_type = str(row['StopType']).strip() # Chuyển về string và xóa khoảng trắng thừa
        
        if stop_type == 'Bến xe':
            if is_on_trip:
                # ==> KẾT THÚC HÀNH TRÌNH
                # Đang đi mà gặp 'Bến xe' -> Đây là lần đầu gặp lại Bến xe
                duration = curr_time - trip_start_time
                
                # Lưu thông tin hành trình
                all_trips.append({
                    'vehicle': vehicle_id,
                    'start_time': trip_start_time,
                    'end_time': curr_time,
                    'duration_seconds': duration.total_seconds(),
                })
                
                # Reset trạng thái
                is_on_trip = False
            
            # Cập nhật thời gian bến xe gần nhất (để dùng làm start_time khi xe rời bến)
            last_station_time = curr_time
            
        else:
            # Đây KHÔNG PHẢI là Bến xe (Trụ dừng, Nhà chờ, Ô sơn...)
            if not is_on_trip:
                # ==> BẮT ĐẦU HÀNH TRÌNH
                # Trước đó chưa đi (đang ở bến), giờ gặp dòng khác bến
                # Theo yêu cầu: Tính từ dòng 'Bến xe' cuối cùng (chính là last_station_time)
                
                if last_station_time is not None:
                    trip_start_time = last_station_time
                    is_on_trip = True
                else:
                    # Trường hợp dữ liệu bắt đầu ngay ở ngoài đường mà chưa thấy Bến xe nào trước đó
                    # Bỏ qua, không tính được điểm bắt đầu
                    pass

# 4. Tạo DataFrame từ danh sách hành trình
trips_df = pd.DataFrame(all_trips)

if not trips_df.empty:
    # 5. Tính trung bình thời gian di chuyển của từng xe
    # Group by xe và tính mean của cột duration_seconds
    avg_duration = trips_df.groupby('vehicle')['duration_seconds'].mean().reset_index()
    avg_duration.rename(columns={'duration_seconds': 'avg_seconds'}, inplace=True)

    # Gộp (Merge) lại bảng hành trình với bảng trung bình
    result_df = pd.merge(trips_df, avg_duration, on='vehicle')

    # 6. LOG RA KẾT QUẢ: Lọc các hành trình > trung bình
    long_trips = result_df[result_df['duration_seconds'] > result_df['avg_seconds']]

    print("=== TỔNG HỢP: CÁC HÀNH TRÌNH TỐN NHIỀU THỜI GIAN HƠN TRUNG BÌNH ===")
    # Chọn các cột cần hiển thị
    output = long_trips[['vehicle', 'start_time', 'end_time', 'duration_seconds', 'avg_seconds']]
    
    # Làm đẹp hiển thị (đổi avg_seconds sang phút cho dễ đọc)
    output['avg_minutes'] = output['avg_seconds'] / 60
    
    print(output[['vehicle', 'start_time', 'duration_seconds', 'avg_minutes']].to_string())
    
    # Lưu ra file nếu cần
    output.to_csv('long_trips_report.csv', index=False)
    print("\nĐã lưu kết quả chi tiết vào file 'long_trips_report.csv'")

else:
    print("Không tìm thấy hành trình nào thỏa mãn điều kiện (Bến xe -> Đi -> Bến xe).")