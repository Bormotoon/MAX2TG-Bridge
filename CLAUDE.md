# MAX2TG-Bridge — контекст для AI-ассистентов

Шпаргалка для будущих сессий: что это, как устроено, где подводные камни.

## Что это

Двусторонний мост MAX (`ws-api.oneme.ru`) ↔ Telegram через форум-топики супергруппы. Каждый MAX-чат = свой topic. Userbot подключается WebSocket'ом к MAX по `__oneme_auth` токену, Telegram-side — обычный bot через python-telegram-bot polling.

Основан на [Aist/max2tg](https://github.com/Aist/max2tg), переписан вокруг форум-топиков. Лицензия MIT.

**Важно про историю:** в сентябре 2026 проект прошёл аудит надёжности (`AUDIT.md`) и был переписан под его выводы. Ядро — durable-журнал доставки. Если правишь доставку, дедупликацию, порядок или жизненный цикл соединения, сначала прочитай раздел «Модель доставки» ниже: большинство «очевидных упрощений» здесь уже пробовали, и они были причиной потери сообщений.

## Структура

```
app/
  main.py          # сборка, сигналы, graceful shutdown
  config.py        # Settings + полная валидация до открытия сокетов
  errors.py        # ConnectionLost, Outcome, SendResult — словарь отказов
  store.py         # SQLite: журнал, части доставки, топики, манифест
  pipeline.py      # планировщик: воркер на чат, ретраи, восстановление
  max_client.py    # WS MAX: state machine, RPC, watchdog, upload/download
  max_listener.py  # MAX → TG: группировка альбомов, рендер, вложения
  tg_handler.py    # TG: команды + быстрый приём апдейтов в журнал
  tg_outbound.py   # TG → MAX: аплоады и отправка
  tg_sender.py     # отправка в TG с явным результатом, нарезка HTML/альбомов
  topics.py        # TopicService — единственный путь создания топиков
  resolver.py      # кеши контактов/чатов с TTL и single-flight
  net.py           # политика исходящих HTTP, спул с лимитами
  health.py        # состояние связи и счётчики
  healthcheck.py   # проба готовности для Docker
  logging_utils.py # редактирование секретов
tests/             # 736 pytest, asyncio_mode=auto
state/             # runtime: bridge.sqlite3 + spool (gitignored)
```

## Модель доставки

Это центральная часть. Порядок операций не случайный.

1. Кадр из WS **валидируется** (`validate_message`). Невалидный — в карантин, не фатально.
2. Событие пишется в **журнал** (`store.enqueue`) с ключом `(source, account, chat_key, event_id, revision)`. UNIQUE — это и есть дедуп.
3. **Воркер чата** (`pipeline._work`) берёт самую старую незавершённую задачу, заглядывает вперёд (`peek_jobs`), решает группировку и **сам** отправляет батч.
4. Результат пишется по частям (`delivery_parts`) с подтверждёнными Telegram message ID.
5. Только после этого задача помечается `delivered`.

Следствия, которые легко сломать:

- **Не помечай сообщение обработанным до доставки.** Ровно это и была главная находка аудита (D01): ID запоминался при приёме, и любой сбой означал тихую потерю — повтор от MAX распознавался как дубль.
- **Ничего не отправляй из фоновой задачи.** Альбомы раньше слал таймер в отдельном task'е, и медленный альбом обгонялся следующим за ним текстом (M01).
- **В альбом попадает только то, что Telegram умеет группировать.** `sendMediaGroup` берёт фото и видео. Если после скачивания в батче оказалось хоть одно вложение не из этого набора — документ, стикер, превью вместо видео, неудавшаяся загрузка, — весь батч уходит по одному сообщению через `_deliver_one`. Раньше такое вложение не попадало ни в ветку «группируемое», ни в ветку «заглушка» и терялось молча, при этом job помечался доставленным.
- **`revision` — это хеш содержимого**, а не счётчик. Тот же ID с тем же содержимым = повтор, с другим = правка. Ревизия намеренно не включает служебные поля вроде счётчиков прочтения, иначе они плодили бы фантомные «правки».
- **`chat_key()` нормализует ID.** MAX непоследователен в int/str; без нормализации один чат получал две записи в карте топиков и два пространства дедупа.
- **`message_map` — отдельный индекс, а не замена `delivery_parts`.** Те же факты есть в частях доставки, но JSON-блобом под job id: ответить «какому сообщению MAX соответствует сообщение Telegram N» можно было только сканом. Заполняется из обоих направлений при подтверждении части, стареет вместе с журналом.

### Три исхода, а не два

`Outcome` в `app/errors.py`: `DELIVERED` / `TRANSIENT` / `PERMANENT` / `UNCERTAIN`.

`UNCERTAIN` — это таймаут на запросе, который Telegram мог принять. Автоповтора нет и быть не должно: у `sendMessage` нет идемпотентного ключа. Такие задачи видны в `/status`, повторяются только руками через `/retry`.

**Частичная доставка — тоже `UNCERTAIN`.** Если первая часть длинного текста ушла, а вторая нет; если первый чанк альбома принят, а второй отвалился; если медиа доставлено, а хвост подписи — нет, то `TRANSIENT` повысится до `UNCERTAIN` (`tg_sender.partial_failure`). Иначе автоповтор перепостит то, что Telegram уже принял. Если не подтверждено вообще ничего, дублировать нечего — исход остаётся `TRANSIENT` и задача повторяется сама.

## Жизненный цикл соединения MAX

`LinkState`: `disconnected → connecting → handshaking → authenticating → ready`, плюс `reconnect_backoff`, `auth_required`, `stopping`, `stopped`.

- `ready` ставится **только** после валидного AUTH_SNAPSHOT с профилем. Открытый сокет — не готовность.
- У connect / handshake / auth / записи в сокет / RPC — отдельные дедлайны. Раньше авторизация могла висеть вечно (R04).
- **Обрыв завершает pending RPC исключением `ConnectionLost`, а не `cancel()`.** Отмена пробивалась `CancelledError` в воркер чата, который ловил только `Exception`, — воркер умирал, очередь оставалась, чат замолкал до рестарта (R01). Это самая дорогая ошибка из всех найденных.
- **Watchdog — отдельный task.** Раньше зависшая запись останавливала heartbeat-loop, который и должен был заметить зависание (R05).
- Пока MAX не ответил на наш ping, действует более мягкий таймаут `SILENT_SERVER_TIMEOUT_SEC` вместо полного отключения контроля.
- `auth_required` ставится **только** при распознанном отказе (`_is_auth_rejection`) или при серверном `LOGOUT` (опкод 20). Молчание — это «авторизация не завершилась», а не «токен невалиден». Разница важна: не надо гонять человека менять рабочий токен из-за сетевого сбоя.
- **`_handle_frame` пропускает `AuthFailed`/`ConnectionLost` наружу.** Всё остальное он глушит в карантин, и когда-то под эту же гребёнку попадал `LOGOUT`: мост оставался в `ready` на сокете, от которого MAX уже отказался.
- Каждое соединение — объект `_Connection` со своим поколением, seq и pending. Поздний `finally` старого соединения не может удалить future нового (R03).

## Протокол MAX (наши находки)

WebSocket: `wss://ws-api.oneme.ru/websocket`, `Origin: https://web.max.ru`.
Пакет: `{"ver":11,"cmd":0,"seq":N,"opcode":OP,"payload":{...}}`. Ответ: `cmd=1` (OK) или `cmd=3` (error).

| Opcode | Назначение |
|---|---|
| 1 | HEARTBEAT_PING |
| 6 | HANDSHAKE |
| 19 | AUTH_SNAPSHOT (логин + первая страница чатов) |
| 20 | LOGOUT (приходит сервером — сессия завершена) |
| 32 | CONTACT_GET — `{names, baseUrl, photoId}`, **НЕ возвращает phone/about** |
| 35 | CONTACT_PRESENCE |
| 48 | CHAT_GET — используем для чатов вне snapshot-страницы |
| 49 | CHAT_HISTORY — `{chatId, from: <ms>, forward: 0, backward: N, getMessages: true}` → `{messages: [...]}`, тела как в DISPATCH. **Проверено** 2026-10 (докачка). Опкод 50 (READ_MESSAGE) рядом не трогать — пометит прочитанным |
| 57 | open by link — только `/join/<token>`; `/u/<token>` → `not.found` |
| 64 | SEND_MESSAGE |
| 65 | ATTACH_TYPING |
| 67 | EDIT_MESSAGE |
| 80 | PHOTO_UPLOAD_URL → `{url}` → POST → `{photos:{…:{token}}}` |
| 82 | VIDEO_UPLOAD_URL |
| 83 | VIDEO_DOWNLOAD_URL |
| 84/85 | **CALLS**, не аудио (проверено) |
| 87 | FILE_UPLOAD_URL → POST → ждать DISPATCH `op=136` |
| 88 | FILE_DOWNLOAD_URL |
| 128 | DISPATCH |
| 130 | READ_MARK |
| 136 | UPLOAD_READY |

### Форматирование

`elements`: `{type, from, length}`, где `from` — **codepoint**-индекс (Telegram даёт UTF-16, конвертация в `tg_handler.utf16_to_char_offset`).

| TG entity | MAX element |
|---|---|
| BOLD | `STRONG` |
| ITALIC | `EMPHASIZED` |
| STRIKETHROUGH | `STRIKETHROUGH` |
| UNDERLINE | `UNDERLINE` |
| CODE / PRE | `MONOSPACED` |
| TEXT_LINK | `LINK` + `attributes.url` |

`CODE_BLOCK`, `BLOCKQUOTE`, `HEADING` отвергаются валидацией (`No enum constant`) — цитаты TG→MAX не переносятся, это задокументировано в `/help`. В обратную сторону (`render_max_text`) MAX-элементы рендерятся в HTML Telegram, включая `BLOCKQUOTE`.

### Осторожно с пробингом

`proto.payload`-ошибки **закрывают WS** для части опкодов. Поэтому перебор непроверенных опкодов на боевом соединении убран из `/profile` (T08), а эвристический перебор CDN-URL для голосовых спрятан за `MAX_AUDIO_PROBING=false` (M09).

## Telegram-side нюансы

- Бот должен быть админом супергруппы с правом «Управление темами», группа — forum-enabled. `sender.preflight()` проверяет это при старте.
- 20 МБ — лимит на **скачивание** ботом через `getFile` (направление TG → MAX). Отправлять мост может больше.
- Custom-emoji реакции ботам запрещены — только 👀.
- Лимит 4096 считается по **видимому тексту**, entity не считаются. `split_html` это учитывает и режет по границам разметки, восстанавливая открытые теги в следующей части.
- **Темп в группу:** `TelegramSender` держит token bucket (15/мин, всплеск 5) — на 20/мин докачка всё равно ловила `RetryAfter`, а медленные ответы под флудом превращались в `UNCERTAIN`. Таймауты HTTP подняты (read 30 с, upload 120 с; дефолт PTB 5 с).
- **В `sendMediaGroup` файлы обязаны быть `InputFile(..., attach=True)`** (`MediaItem.input_file(attach=True)`), иначе «media not found». До октября 2026 альбомы не работали вообще — фейковый бот в тестах этого не видел; `TestAlbumWireFormat` проверяет `attach_uri`.
- Одно сообщение MAX с несколькими фото/видео идёт альбомом (`_is_single_message_album`), если нет цитаты/пересылки/правки и в топике ещё ничего не могло оказаться.
- Ответы шлются с `allow_sending_without_reply=True`: оригинал могли удалить в Telegram.
- Доставка MAX→TG ждёт `ready` до 30 с (`READY_WAIT_SEC`), иначе имена не резолвятся и в заголовке ID. Дольше не ждёт — чтобы не сжечь попытки при долгом простое MAX.
- `sendMediaGroup` не принимает один элемент — `chunk_media` балансирует части (11 → 6+5), чтобы одиночки не возникало (M03).

## Команды

`/bind`, `/add`, `/profile`, `/intro`, `/del`, `/status`, `/retry`, `/help`. Все — через `authorize()`, fail-closed: без `TG_ALLOWED_USER_ID` или явного `TG_ALLOW_ANY_MEMBER=true` мост не стартует.

## Безопасность

- **Логи.** `logging_utils.RedactingFilter` вешается на хендлеры до первой записи. Токены, поля `token`/`deviceId`/`photoToken`, query-строки подписанных URL вырезаются. Тексты сообщений не пишутся без `LOG_MESSAGE_TEXT=true`.
- **Исходящие HTTP.** `net.validate_url` проверяет схему, хост по allowlist и резолвленные адреса (приватные/loopback/link-local — отказ), редиректы разбираются вручную с повторной проверкой каждого хопа. Остаточный риск DNS-rebinding задокументирован в docstring `net.py`.
- **Загрузки** идут потоком в спул с лимитом на файл и общим бюджетом; лимит считается по фактически прочитанным байтам, а не по `Content-Length`.

## Тесты

`pytest -q` → **736 passed** (через `.venv/bin/python -m pytest`, системного `python` в PATH нет). `ruff check app tests` → clean.

Особо ценные:
- `tests/test_max_connection.py` — против настоящего локального WS-сервера: дедлайны handshake/auth, отказ токена vs ошибка сервера, серверный LOGOUT, обрыв во время RPC, битые кадры, реконнект, отсутствие реконнект-лупа на idle-связи.
- `tests/test_pipeline.py` — ретраи, порядок, dead-letter, восстановление после падения, uncertain.
- `tests/test_store.py` — ключи дедупа, уникальность топиков в обе стороны, compare-and-delete.
- `tests/test_net.py::TestSafeFetcher` — против локального HTTP-сервера: каждый редирект проверяется заново, лимит байт держится на chunked-ответе без `Content-Length`, спул не течёт после отказа.
- `tests/test_logging_redaction.py::TestTheRealLogFile` — реальный `setup_logging`, запись на диск, проверка файла на sentinel-токен. Изолированные тесты `redact()` не ловят незацепленный хендлер.
- `tests/test_max_listener.py::TestMixedAlbumBatch` — вложение, которое альбом не увезёт, не должно исчезать.
- `tests/test_main_wiring.py::TestOutageWarning` — реальный `run_bridge` против локального WS-сервера: обрыв, реконнект, уведомление о пропуске. Короткий блип уведомления не даёт.

**Пиши тест от сценария отказа, а не от реализации.** Пара примеров, как это ломалось: тест LOGOUT дёргал `_dispatch()` напрямую — а глушил исключение `_handle_frame()`, единственный реальный вызывающий. Тест orphan-топиков дописывал маркер *после* успешной привязки, поэтому проходил на реализации, которая сканировала таблицу топиков — там, где топик реально теряется, строки в ней нет.

Предпочитай детерминированные Event/условия вместо `sleep()` — в тестах есть хелпер `tests.conftest.drain`. Ретраи `TelegramSender` настраиваются через `RETRY_BACKOFF_SEC` (в тестах = 0), чтобы не ждать по-настоящему.

## Деплой

Docker, один базовый образ `python:3.12-slim` на обе стадии (раньше site-packages копировались из glibc в musl — O01). Контейнер не от root (UID 10001), read-only rootfs, лимиты памяти/pids, `stop_grace_period: 40s`.

Прод крутится **на этой же машине** (`docker compose` в корне репо), а не на удалённом хосте. `state/` и `logs/` должны принадлежать UID 10001.

**Сеть хоста:** роутер раздаёт fake-IP DNS (TUN-прокси) — все имена резолвятся в `198.18.0.0/15`. Поэтому в `.env` стоит `MEDIA_TRUSTED_NETS=198.18.0.0/15`, иначе `net.validate_url` отвергает все медиа-URL как «non-public». Тесты не должны зависеть от DNS машины — резолвинг в `tests/test_net.py` подменяется.

**Протухший токен MAX** выглядит так: handshake проходит, на AUTH_SNAPSHOT приходит `cmd=3` с `error: login.token` / `FAIL_LOGIN_TOKEN`. Мост уходит в `auth_required` и шлёт «⛔ MAX отклонил токен». Лечится только новым `__oneme_auth` + `__oneme_device_id` из web.max.ru и `docker compose up -d --force-recreate`.

`docker compose` сам **не перезапускает** unhealthy-контейнер — healthcheck нужен для `docker ps` и внешнего мониторинга.

## Что осталось / known issues

- Голосовые TG → MAX уходят `.ogg`-файлом. Нужен захват сети web.max.ru, чтобы найти нативный опкод.
- Голосовые MAX → TG для `_type=UNSUPPORTED`: пробуем опкод 88, иначе явная пометка.
- `/u/<token>` не резолвится опкодом 57.
- Phone/about не отдаёт `CONTACT_GET`.
- **Catch-up после простоя не встроен в мост**, но опкод 49 подтверждён. В октябре 2026 докачка 110 сообщений сделана разовыми скриптами: история через отдельное WS-соединение (не боевое) → `validate_message` → `store.enqueue` с `payload["silent"]=True` при остановленном мосте → старт. Дедуп по `(chat, id, revision)` работает как для живых. Флаг `silent` в payload → `tg_sender.silent()` → `disable_notification` во всех вызовах Bot API. Граница — последнее «New message: id=» в старом логе.
- **Прежнее описание (до проверки опкода 49):** нет подтверждённого history/sync механизма, курсора нет. Зависим от replay со стороны MAX. Что сделано вместо него: разрыв дольше `main.GAP_NOTICE_SEC` (120 с) после восстановления даёт отдельное уведомление «сообщения за это время дойдут, только если MAX сам их повторит», и длительность последнего разрыва видна в `/status`. Это уведомление намеренно **не** проходит через throttle инцидентов: предупреждение о возможной потере данных нельзя глушить из-за того, что недавно было другое.
- **Обновление токена не исследовано.** Порядок исследования расписан в `AUDIT.md` §14.6. Не пробовать непроверенные опкоды на боевом соединении.
- **Reply TG → MAX ждёт подтверждения формата.** Сопоставление «сообщение TG ↔ сообщение MAX» реализовано и протестировано (таблица `message_map`, индекс в обе стороны, заполняется из обоих направлений). Не подтверждена только форма поля `link` в SEND_MESSAGE, поэтому отправка сидит за `MAX_REPLY_LINK=false`. Чтобы закрыть: снять трафик web.max.ru на отправке ответа (AUDIT.md §14.6), поправить возвращаемый dict в `TelegramToMaxHandler._reply_link`, включить флаг по умолчанию.

## Если нужно ребутнуть знание о репо

```bash
cd /home/borm/VibeCoding/max-tg
git status && pytest -q && ruff check app tests

# Прод — локальный docker compose в этом же каталоге (контейнер max-tg-max2tg-1)
docker compose ps && docker compose logs --tail=50 max2tg
```

## Ссылки

- Upstream: [Aist/max2tg](https://github.com/Aist/max2tg)
- Опкоды: [nsdkinx/vkmax](https://github.com/nsdkinx/vkmax) ([docs/opcodes.md](https://github.com/nsdkinx/vkmax/blob/main/docs/opcodes.md))
- Официальный бот-API MAX: [max-messenger/max-botapi-python](https://github.com/max-messenger/max-botapi-python) — там `enums/text_style.py`
- Альтернативный мост: [mimimiartartart/MaxToTelegramBridge](https://github.com/mimimiartartart/MaxToTelegramBridge)
- Аудит, по которому переписан проект: `AUDIT.md`
