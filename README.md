# Black Server My System

یک سرور دانلود فایل عمومی برای ویندوز — بدون نیاز به هیچ تنظیم شبکه، پورت فوروارد، یا آدرس IP ثابت.
فایل `start_server.bat` را دابل‌کلیک کنید تا پوشه `downloads` شما به یک لینک عمومی تبدیل شود.

اگر کاربر ایرانی هستید، حتما فیلترشکن روشن کنید!


A public file download server for Windows — no port forwarding, no public IP,
no router configuration needed. Double-click `start_server.bat` and your
`downloads` folder becomes a public download link.

```
Internet
   |
   v
Cloudflare Quick Tunnel / SSH Fallback  (cloudflared / localhost.run / serveo)
   |
   v
127.0.0.1:8080                           (local file server)
   |
   v
./downloads/
```

---

## فارسی

### ویژگی‌ها

- اجرای آسان فقط با یک دابل‌کلیک
- سرور فایل محلی فقط روی `127.0.0.1` (هرگز مستقیماً در اینترنت قابل دسترس نیست)
- فقط پوشه `downloads/` سرو می‌شود — هیچ فایل دیگری قابل دسترس نیست
- نمایش لیست فایلها با سایز خوانا
- دانلود استریمینگ (فایلها کامل در RAM بارگذاری نمی‌شوند)
- پشتیبانی از Range (امکان ادامه دانلود ناقص)
- دانلود خودکار `cloudflared.exe` در صورت نبود
- حفاظت از اجرای همزمان چند نمونه
- خروج تمیز بدون پروسه یتیم
- لاگ‌ها در `logs/server.log` و `logs/cloudflared.log`

### نیازها

- ویندوز 10/11 (64 بیت)
- پایتون 3.9 یا جدیدتر
- اتصال اینترنت

### نحوه اجرا

1. فایل `start_server.bat` را **دابل‌کلیک** کنید
2. اگر پایتون یا cloudflared نصب نباشد، **پنجره نصب** باز می‌شود و همه چیز را نصب می‌کند
3. بعد از نصب، سرور خودکار اجرا می‌شود
4. لینک عمومی در مرورگر باز می‌شود و در کلیپ‌بورد کپی می‌شود

### نحوه استفاده

فایلهایی که می‌خواهید به اشتراک بگذارید را در پوشه `downloads/` کپی کنید.
آنها فوراً در لینک عمومی قابل دسترس هستند — نیازی به ریستارت نیست.

### نحوه توقف

`Ctrl+C` را در پنجره ترمینال فشار دهید.

### ذخیره‌سازی روی Neon (دیتابیس ابری)

به‌جای دیسک محلی، می‌شود فایل‌ها را روی [Neon](https://neon.com) نگه داشت:
بایت‌های فایل در **Object Storage** و مشخصات (نام، سایز، تاریخ، مالک) در **Postgres**.
در این حالت هر آپلود، هر ویرایش، هر ساخت فایل یا پوشه و هر حذف، مستقیم در Neon ذخیره
می‌شود و سایت در هر بار اجرا فایل‌ها را از همان‌جا می‌خواند.

**۱) در پنل Neon بسازید:**
- یک **Storage bucket** (مثلاً با نام `files`) و کلید دسترسی آن
- رشتهٔ اتصال **Postgres** (از بخش Connection، با `?sslmode=require`)

**۲) در فایل `.env` کنار `server.py` پر کنید:**
```
AWS_ENDPOINT_URL_S3=https://<account>.storage.neon.tech
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=<key>
AWS_SECRET_ACCESS_KEY=<secret>
BUCKET_NAME=files
DATABASE_URL=postgresql://<user>:<password>@<host>/<db>?sslmode=require
BS_STORAGE=neon
```
`BS_STORAGE=neon` یعنی سرور **هرگز** به دیسک محلی برنگردد؛ اگر پیکربندی ناقص باشد
اصلاً بالا نمی‌آید. (بدون این خط، حالت `auto` است: تا وقتی کامل پر نشود، سایت روی دیسک محلی کار می‌کند.)

**۳) بررسی اتصال و انتقال فایلهای موجود:**
```
python server.py --check-storage      # تست bucket و دیتابیس
python server.py --migrate-dry-run    # نمایش فایلهایی که منتقل می‌شوند
python server.py --migrate            # انتقال پوشه downloads به Neon
```
`--migrate` فایلها را در Neon کپی می‌کند و از پوشهٔ محلی **پاک نمی‌کند**، پس اگر
اشتباهی شد چیزی از دست نمی‌رود. بعد از آن سایت فقط از Neon می‌خواند.

### گزینه‌های خط فرمان

| گزینه | توضیح |
|--------|--------|
| `--port N` | پورت محلی (پیش‌فرض `8080`، خودکار تغییر می‌کند) |
| `--no-tunnel` | فقط سرور محلی، بدون تونل |
| `--no-browser` | مرورگر خودکار باز نشود |
| `--tunnel-timeout N` | ثانیه انتظار برای لینک عمومی (پیش‌فرض 60) |
| `--register-timeout N` | ثانیه انتظار برای اتصال دیتاپلین (پیش‌فرض 30) |
| `--protocol P` | پروتکل تونل: `auto`، `quic` یا `http2` |
| `--no-ssh-fallback` | غیرفعال کردن SSH fallback |
| `--ssh-timeout N` | ثانیه انتظار برای SSH fallback (پیش‌فرض 45) |
| `--check-storage` | بررسی اتصال Neon (bucket و دیتابیس) و خروج |
| `--migrate` | انتقال فایلهای پوشه `downloads` به Neon و خروج |
| `--migrate-dry-run` | فقط نشان می‌دهد چه چیزی منتقل می‌شود |
| `--verbose` | نمایش لاگهای دیباگ |

مثال:
```
start_server.bat --port 9000
start_server.bat --no-tunnel
start_server.bat --no-browser
```

### رفع ارورها

| ارور | راه حل |
|------|--------|
| "Python was not found" | پایتون 3 را نصب کنید و گزینه "Add python.exe to PATH" را بزنید |
| دانلود cloudflared ناموفق بود | اینترنت را چک کنید یا `cloudflared.exe` را دستی از [GitHub](https://github.com/cloudflare/cloudflared/releases/latest) دانلود کرده و در پوشه `bin/` قرار دهید |
| پورت اشغال است | سرور خودکار پورت جایگزین انتخاب می‌کند |
| "Another instance is running" | پنجره دیگر را ببندید یا فایل `logs/server.lock` را حذف کنید |
| لینک عمومی نمایش داده نمی‌شود | `logs/cloudflared.log` را بررسی کنید؛ اینترنت را چک کنید |
| لینک رزرو شده ولی 530 می‌دهد | شبکه شما ترافیک cloudflared را بلاک می‌کند. خودکار به SSH fallback می‌رود |
| SSH fallback کار نمی‌کند | هر دو provider (localhost.run + serveo) امتحان می‌شوند. اگر هیچکدام کار نکرد، ISP شما SSH را بلاک کرده — از VPN full-tunnel استفاده کنید |
| لینک عمومی کار نمی‌کند روی دستگاه دیگر | لینک Cloudflare موقتی است و با هر بار restart عوض می‌شود. لینک جدید را دوباره بفرستید |

### امنیت

- سرور فقط روی `127.0.0.1` بایند می‌شود
- فقط `downloads/` سرو می‌شود — فرار از مسیر و symlink رد می‌شود
- فایلهای مخفی (`.env`، `.gitignore` و...) سرو نمی‌شوند
- فقط GET/HEAD — آپلود یا پنل مدیریت وجود ندارد
- فایلهای `bin/`، `logs/`، `server.py` و... قابل دسترس نیستند
- فایلهای حساس را در `downloads/` قرار ندهید — همه چیز عمومی است

### ساختار پروژه

```
project/
├── start_server.bat          # فایل اجرایی اصلی
├── server.py                 # سرور فایل + مدیریت تونل
├── check_requirements.py     # بررسی و نصب خودکار نیازها
├── requirements.txt          # بدون dependency خارجی
├── README.md
├── .gitignore
├── bin/
│   └── cloudflared.exe       # کلاینت تونل Cloudflare
├── downloads/                # پوشه عمومی
│   └── .gitkeep
└── logs/
    ├── server.log
    └── cloudflared.log
```

---

## English

### Features

- One double-click to go online.
- Local file server bound to `127.0.0.1` only (never exposed directly).
- Only the `downloads/` folder is served — nothing else is reachable.
- Clean directory listing with human-readable file sizes.
- Streaming downloads (files are never loaded fully into RAM).
- HTTP Range support, so large downloads can be resumed.
- Automatic `cloudflared.exe` download when it is missing.
- Automatic SSH fallback (localhost.run + serveo.net) when Cloudflare is blocked.
- Single-instance protection, clean shutdown, no orphan processes.
- Logs in `logs/server.log` and `logs/cloudflared.log`.

### Requirements

- Windows 10/11 (64-bit).
- Python 3.9+ installed and available as `python` or `py`.
  Download: <https://www.python.org/downloads/> (tick *Add python.exe to PATH*).
- An internet connection for the Cloudflare tunnel.

No third-party Python packages are needed (`requirements.txt` is intentionally
empty of dependencies).

### How to run

1. **Double-click** `start_server.bat`.
2. If Python or cloudflared is missing, an **install window** opens automatically
   and installs everything.
3. After installation, the server starts automatically.
4. The public URL opens in your browser and is copied to your clipboard.

### How to use

Copy any files you want to share into the `downloads/` folder.
They appear immediately at the public URL — no restart needed. Subfolders are
supported too.

### How to stop

Press `Ctrl+C` in the terminal window.

### Command line options

You can pass options through the launcher, for example:

```
start_server.bat --port 9000
start_server.bat --no-tunnel
start_server.bat --no-browser
```

| Option                   | Description                                        |
| ------------------------ | -------------------------------------------------- |
| `--port N`               | Preferred local port (default `8080`; auto-fallback)|
| `--no-tunnel`            | Local server only, no Cloudflare tunnel            |
| `--no-browser`           | Do not open the browser automatically              |
| `--tunnel-timeout N`     | Seconds to wait for the public URL (default 60)    |
| `--register-timeout N`   | Seconds to wait for the tunnel data plane to connect (default 30) |
| `--protocol P`           | cloudflared transport: `auto`, `quic` or `http2`   |
| `--no-ssh-fallback`      | Disable the automatic SSH fallback tunnel          |
| `--ssh-timeout N`        | Seconds to wait for the SSH fallback (default 45)  |
| `--check-storage`        | Check the Neon bucket and database, then exit      |
| `--migrate`              | Copy the files in `downloads` into Neon, then exit |
| `--migrate-dry-run`      | Show what `--migrate` would copy, write nothing    |
| `--verbose`              | Print debug logs to the console                    |

### Storing the files on Neon

Instead of the local disk the site can keep everything in
[Neon](https://neon.com): the file bytes in **Object Storage**, the metadata
(name, size, date, owner) in **Postgres**. Every upload, edit, new file, new
folder and delete is then written straight to Neon, and the site reads the
file list from there on every start.

1. In the Neon console create a **storage bucket** (e.g. `files`) and an
   access key, and copy the **Postgres connection string**.
2. Fill in `.env` next to `server.py`:

   ```
   AWS_ENDPOINT_URL_S3=https://<account>.storage.neon.tech
   AWS_REGION=us-east-2
   AWS_ACCESS_KEY_ID=<key>
   AWS_SECRET_ACCESS_KEY=<secret>
   BUCKET_NAME=files
   DATABASE_URL=postgresql://<user>:<password>@<host>/<db>?sslmode=require
   BS_STORAGE=neon
   ```

   `BS_STORAGE=neon` means the server never falls back to the local disk —
   with an incomplete configuration it refuses to start. Without that line
   the default is `auto`: the site uses Neon once it is fully configured and
   the local disk until then.
3. Check the setup and move the files that are already on disk:

   ```
   python server.py --check-storage
   python server.py --migrate-dry-run
   python server.py --migrate
   ```

   `--migrate` copies the files into Neon and leaves the local folder alone,
   so nothing is lost if it was run by accident.

### Troubleshooting

| Symptom                         | Fix                                                        |
| ------------------------------- | ---------------------------------------------------------- |
| "Python was not found"          | Install Python 3 and tick *Add python.exe to PATH*.        |
| cloudflared download fails      | Check your internet, or download `cloudflared.exe` manually from the [official releases page](https://github.com/cloudflare/cloudflared/releases/latest) into `bin/`. |
| Port already in use             | The server automatically picks a free port.                |
| "Another instance is running"   | Stop the other window, or delete `logs/server.lock`.       |
| No public URL appears           | Check `logs/cloudflared.log`; verify your internet works.  |
| URL reserved but link returns error/530 | Your network blocks `cloudflared` traffic (UDP/TCP port `7844`). The server auto-falls back to SSH tunnel providers. |
| SSH fallback not working | Both providers (localhost.run + serveo.net) are tried automatically. If both fail, your ISP blocks SSH — use a full-tunnel VPN. |
| Link doesn't work on another device | Cloudflare URLs are temporary and change on each restart. Send the new link again. |

### Project layout

```
project/
├── start_server.bat          # double-click launcher
├── server.py                 # file server + tunnel orchestration
├── check_requirements.py     # automatic requirements checker/installer
├── requirements.txt          # no external dependencies
├── README.md
├── .gitignore
├── bin/
│   └── cloudflared.exe       # Cloudflare tunnel client (included)
├── downloads/                # the only public folder
│   └── .gitkeep
└── logs/
    ├── server.log
    └── cloudflared.log
```
