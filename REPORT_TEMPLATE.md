# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đoàn Minh Hiếu  **Lớp:** AICB-P2T2  **Ngày:** 17/8/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
```

</details>

Tổng kết: **… / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` tăng số lượng hàng liên tục sau mỗi lần chạy lại (khi bấm Clear Task). |
| **Nguyên nhân** | Model dbt được cấu hình là `incremental` nhưng không khai báo `unique_key`. Khi dbt chạy ở chế độ này, mặc định nó sẽ sinh ra câu lệnh `INSERT` (append) để thêm dữ liệu. Nguồn CDC chứa các bản ghi cập nhật (`op='u'`) với cùng một ticket ở nhiều ngày khác nhau. Khi chạy lại, dbt liên tục insert thêm các bản ghi này thay vì ghi đè, dẫn đến hiện tượng nhân bản dữ liệu. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`: Khai báo thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Trong `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1` để giới hạn số lượt chạy lại đồng thời. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: giống hệt nhau |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu hàng ở những ngày quá khứ, trong khi ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2.72 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | 3 ngày — vì 99% bản ghi đến hệ thống trong vòng 3 ngày sau sự kiện, nên lùi lại 3 ngày sẽ vét được gần như toàn bộ dữ liệu muộn. |
| **Nguyên nhân** | Model sử dụng điều kiện `is_incremental()` là `event_date > max(event_date)`. Mỗi lần chạy, model chỉ xử lý các ngày lớn hơn ngày lớn nhất đã có trong đích. Nếu một sự kiện xảy ra ngày 08-12 nhưng đến kho muộn vào ngày 08-15, lúc này max(event_date) đã là 08-15, bản ghi ngày 08-12 bị điều kiện `>` loại bỏ hoàn toàn, không bao giờ được tính vào bảng target. |
| **Cách khắc phục** | Sửa `gold_feature_daily.sql` thành `event_date >= (select max(event_date) from {{ this }}) - interval 3 day` để tính toán lại 3 ngày quá khứ. Đồng thời cấu hình `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để khi tính lại không gây trùng lặp. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Dùng `max` sẽ tốn chi phí tính toán rất lớn: ví dụ nếu 1 bản ghi duy nhất đến trễ 30 ngày, mọi lượt chạy sau đó đều phải tính lại toàn bộ 30 ngày quá khứ. Dùng `P99` là sự đánh đổi hợp lý: quét vừa đủ cửa sổ (lookback window) để lấy 99% dữ liệu, tiết kiệm đáng kể tài nguyên kho dữ liệu. 1% cực hạn có thể được xử lý bằng script backfill định kỳ.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `priority` có rất nhiều dòng NULL và chứa các số không hợp lệ (0, 5, -1). Mô hình phân loại dự đoán kém do không nhận được đủ dữ liệu mức ưu tiên, và pipeline không hề phát ra cảnh báo. |
| **Nguyên nhân** | Hệ thống nguồn thay đổi (schema evolution) thành chuỗi `urgent` thay vì số `1`, đồng thời chứa các rác như `unknown`, `0`, rỗng. Hàm `try_cast` hiện tại chuyển chữ `urgent` thành `null` (gây mất dữ liệu) nhưng lại cho phép số `0` đi qua (sai contract). Chưa có cơ chế bắt và cách ly dòng lỗi. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1 (1..4): Giữ nguyên kiểu integer.<br>Nhóm 2 ('urgent', 'high',...): Dùng khối CASE WHEN map về số (1, 2, 3, 4) do đây là schema evolution hợp lệ.<br>Nhóm 3 (0, 5, P1, rỗng, null): Trả về NULL để nhận diện là dữ liệu lỗi cần cách ly. |
| **Cách khắc phục** | - `normalize_priority.sql`: Sửa macro dùng CASE WHEN để phân loại 3 nhóm như trên.<br>- `silver_tickets.sql`: Thêm `WHERE normalize_priority('priority_raw') is not null` trước khi rank `row_number` để không mất trạng thái cũ của ticket.<br>- `quarantine_tickets.sql`: Bắt các bản ghi lỗi bằng điều kiện ngược lại là `is null`.<br>- `schema.yml`: Cấu hình `enforced: true` và `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` pass (tổng > 9 tests) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn lỗi (quarantine) ở Silver thay vì Bronze. Bronze cần lưu dữ liệu thô (raw/immutable) y hệt nguồn để phục vụ việc điều tra sự cố hoặc replay dữ liệu. Nếu chặn ngay ở Bronze, dữ liệu lỗi sẽ mất tích hoàn toàn.<br>Không để pipeline dừng khi gặp bản ghi lỗi (fail-fast toàn bộ) vì số lượng vài trăm dòng lỗi không đáng để bắt toàn bộ hệ thống phải ngưng hoạt động, cản trở việc xử lý hàng chục ngàn dòng dữ liệu đúng. Cơ chế quarantine giúp chia luồng: dữ liệu đúng tiếp tục đi, dữ liệu sai vào bảng chờ xử lý.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A / B / không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Cấu hình `unique_key` và `incremental_strategy` của các bảng được cập nhật incremental, cũng như tham số `catchup` / `max_active_runs` trên trình lập lịch (Airflow). |
| 2 | Biểu đồ phân bố độ trễ giữa thời điểm sinh ra dữ liệu (`event_time`) và thời điểm dữ liệu được nạp vào kho (`ingested_at`) để xác định lookback window hợp lý. |
| 3 | Tần suất xuất hiện của các giá trị NULL hoặc ngoài miền hợp lệ trong các cột quan trọng (contract validation), và kiểm tra xem hệ thống có cơ chế quarantine (cách ly) hay không. |
