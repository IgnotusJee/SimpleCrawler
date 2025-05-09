# 特定网页地址批量抓取 项目说明

## 项目功能特性
1. **动态URL生成引擎**
   
   - 支持数字序列和日期序列变量模板
   - 可配置变量规则（起始值/结束值/步长/格式）
   - 自动生成变量笛卡尔积组合URL
   
2. **智能爬取核心**
   
   - 协程并发下载（可配置并发数）
   - 自动识别 url 爬取数据类型，如 html, json 等
   - 智能存储策略：
     * 按域名自动创建目录结构
     * 自动生成规范文件名
     * 内容哈希去重机制
   
3. **高级特性**
   
   - 日志轮转管理（单日志最大大小可配置，超过自动新建文件并备份老日志）
   - SQLite轻量化存储：
     * 记录爬取元数据
     * 支持快速查重
     * 存储文件路径映射

   - 一键清理系统（日志/下载文件/数据库）

## 配置与使用指南

项目使用 ubuntu 24.04, python 3.12.3 环境，可以通过`pip install -r requirements.txt`配置依赖。

### 配置方式

**1. JSON配置文件**

可以自定义并发数、存储位置、user agent 等，通过`-c`选项传入配置文件位置，也可以留空使用默认的配置选项

```json
{
    "concurrency": 5,
    "retry_policy": {
        "max_attempts": 3,
        "backoff_base": 2
    },
    "storage": {
        "base_dir": "./downloads",
        "db_file": "scraped_data.db",
        "max_file_size": 104857600
    },
    "logging": {
        "log_filename": "scraper.log",
        "backup_count": 5,
        "max_bytes": 10485760
    },
    "user_agent": "Mozilla/5.0 (compatible; BatchScraper/1.0; +https://example.com/bot)"
}
```

**2. 输入方式**

可以直接命令行输入

```bash
python3 craw.py run \
  --pattern "https://spa1.scrape.center/api/movie/{cnt}/" \
  --var-rule cnt,type=number,start=1,end=10,step=1
```

也可以将输入保存在 JSON 文件中，命令行传入文件位置

```json
[
  {
    "url_template": "https://spa1.scrape.center/api/movie/{cnt}/",
    "variables": {
      "cnt": {
        "type": "number",
        "start": 1,
        "end": 10,
        "step": 1
      }
    }
  },
  {
    "url_template": "https://www.biubiu001.com/sjzxd/{num}.html",
    "variables": {
      "num": {
        "type": "number",
        "start": 108450,
        "end": 108486,
        "step": 1
      }
    }
  }
]
```

```bash
python3 craw.py run -i target.json
```

### 运行参数说明

| 参数组   | 参数          | 说明                                                         |
| -------- | ------------- | ------------------------------------------------------------ |
| 核心参数 | -c/--config   | 指定配置文件路径（默认使用内置配置）                         |
|          | -i/--input    | JSON格式的爬取目标配置文件                                   |
|          | -p/--pattern  | 直接指定URL模板                                              |
| 变量规则 | -a/--var-rule | 变量定义语法：`变量名,type=类型,start=起始值,end=结束值,step=步长[,format=格式]` |
| 清理参数 | -a/--all      | 清理所有持久化数据                                           |
|          | -o/--log      | 仅清理日志文件                                               |
|          | -d/--download | 仅清理下载目录                                               |
|          | -D/--database | 仅清理数据库                                                 |

### 典型工作流程

1. 初始化配置（文件或命令行）
2. 生成所有目标URL
3. 并发爬取并存储：
   - 自动识别内容类型
   - 哈希校验内容唯一性
   - 异常自动重试
4. 记录元数据到数据库
5. 支持后续清理操作

该系统通过合理的配置组合，可灵活适应从简单页面抓取到复杂参数化URL爬取等多种场景，其模块化设计也便于进行功能扩展。
