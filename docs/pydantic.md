# Hướng dẫn Thực hành Pydantic V2 & Pydantic Settings

Tài liệu này tóm tắt các thực hành tốt nhất (Best Practices) khi sử dụng `pydantic` (phiên bản 2.x) và `pydantic-settings` trong dự án Python 3.10+.

## 1. Pydantic Core (Data Modeling)

Pydantic V2 viết bằng Rust, mang lại hiệu năng cao và cơ chế validation chặt chẽ.

### Định nghĩa Model cơ bản
Sử dụng type hints chuẩn của Python 3.10 (`list`, `dict`, `|` cho Union).

```python
from pydantic import BaseModel, Field, EmailStr
from datetime import datetime
from typing import Optional

class User(BaseModel):
    id: int
    username: str
    # Field: Cấu hình chi tiết (default, validation)
    email: str = Field(..., pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    # Optional field, default là None
    age: int | None = None
    # Set default factory cho các kiểu mutable (list, dict)
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
```

### Validation nâng cao (Validators)
Trong V2, sử dụng decorator `@field_validator` và `@model_validator`.

```python
from pydantic import BaseModel, field_validator, model_validator

class Transaction(BaseModel):
    amount: float
    currency: str

    # 1. Validate một trường cụ thể
    @field_validator('amount')
    @classmethod
    def check_amount_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError('Amount must be positive')
        return v

    # 2. Validate logic giữa các trường (Cross-field validation)
    @model_validator(mode='after')
    def check_currency_limit(self) -> 'Transaction':
        if self.currency == 'USD' and self.amount > 10000:
            raise ValueError('USD transaction limit exceeded')
        return self
```

### Serialization (Xuất dữ liệu)
Thay vì `.dict()` và `.json()` (đã cũ), hãy dùng `.model_dump()` và `.model_dump_json()`.

```python
tx = Transaction(amount=100, currency="ETH")

# Xuất ra Dictionary
data_dict = tx.model_dump(exclude_none=True)

# Xuất ra JSON string
json_str = tx.model_dump_json(indent=2)
```

### Cấu hình Model (ConfigDict)
Trong V2, dùng `model_config` thay vì class `Config`.

```python
from pydantic import BaseModel, ConfigDict

class SecureModel(BaseModel):
    # frozen=True: Tạo object bất biến (immutable) -> thread-safe, hashable
    # str_strip_whitespace: Tự động trim khoảng trắng string
    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    api_key: str
```

## 2. Pydantic Settings (Quản lý cấu hình)

Tách biệt logic code và cấu hình. Ưu tiên đọc từ Biến môi trường (Environment Variables).

### Setup cơ bản
Class `BaseSettings` sẽ tự động tìm biến môi trường không phân biệt hoa thường (case-insensitive).

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, RedisDsn

class Settings(BaseSettings):
    # Tự động đọc biến APP_NAME, nếu không có thì dùng default
    app_name: str = "Crypto Analyzer"

    # Tự động validate định dạng URL
    database_url: PostgresDsn
    redis_url: RedisDsn = "redis://localhost:6379/0"

    # SecretStr giúp ẩn giá trị khi print log
    api_key: str

    # Cấu hình load file .env
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

# Khởi tạo một lần duy nhất (Singleton pattern thường dùng)
settings = Settings()
```

### Xử lý Nested Configuration (Cấu hình lồng nhau)
Dùng dấu gạch dưới đôi `__` để map biến môi trường vào object con.

```python
class DatabaseConfig(BaseModel):
    host: str
    port: int

class AppSettings(BaseSettings):
    db: DatabaseConfig

# Biến môi trường:
# DB__HOST=localhost
# DB__PORT=5432
```

## 3. Các mẫu thiết kế (Patterns) thường dùng

### Alias (Ánh xạ tên trường)
Dữ liệu nguồn (API, Blockchain) thường dùng `camelCase`, nhưng Python chuẩn là `snake_case`.

```python
class EthBlock(BaseModel):
    block_number: int = Field(alias="number")
    parent_hash: str = Field(alias="parentHash")

    # Cho phép populate bằng cả tên field hoặc alias
    model_config = ConfigDict(populate_by_name=True)

# Input: {"number": 123, "parentHash": "0x..."}
# Object: block.block_number == 123
```

### Computed Fields (Trường tính toán)
Tạo ra trường mới dựa trên dữ liệu có sẵn khi serialize.

```python
from pydantic import ComputedField

class Rect(BaseModel):
    width: int
    height: int

    @ComputedField
    @property
    def area(self) -> int:
        return self.width * self.height
```

### Performance Tip
Nếu cần hiệu năng cao khi parse JSON, cài đặt thư viện `orjson` hoặc `ujson` thì Pydantic sẽ tự động tận dụng.

---
*Lưu ý: Luôn chạy `mypy` để kiểm tra các lỗi về type safety khi làm việc với Pydantic.*
