## Домашнее задание по Hadoop-Hive-Spark

* Soft deadline: 29.11.2020, 23:59 -50%
* Hard deadline: 06.12.2020, 23:59 -100%.

#### Работа с Apache HUE
Для входа в HUE, нужно зайти на кластер с пробросом порта 8888.
```bash
ssh <user>@mipt-client.atp-fivt.org -L 8888:mipt-node03:8888
```
Логины и пароли даны по [ссылке](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ8qODGHmk4DD7uuWLR3RKPfw52zpLRumZ35KLZrLG9ucYKB_sgHseFz-eudWN5FMHM7csEkwcQyyEQ/pubhtml?gid=1278893885&single=true). При 1-м входе в HUE просьба сменить пароль.

Присылать исправления можно в течение месяца после комментариев.

В данном ДЗ нужно решить 2 задачи. 

1. [1 балл]. Задача 110 из параллелок, но сделать её нужно **на Spark**. Код нужно закоммитить в comparisontask1

Список идентификаторов перемешайте в случайном порядке. Далее в каждой строке запишите через запятую случайное число идентификаторов - от 1 до 5.
Вариант способа перемешивания записей: дописать к каждой случайное число, отсортировать по нему весь список, потом число отбросить.

Входные данные: список идентификаторов. /data/ids
Формат вывода: id1,id2,...
Вывод на печать: первые 50 строк.

P.S. Задача из реальной жизни. Нужно было проверить сервер новой БД на нагрузку (“обстрелять”). Для этого нужно было сгенерировать запросы к ней (т.н.”патроны”), которые бы походили на реальные. Был известен список ключей, которые в этой базе могут быть, также было известно, что в одном запросе таких ключей до пяти штук.
Пример вывода:
```
1cf54b530128257d72,4cdf3efa01036a9a48,8c3e7fb30261aaf9cf
4cfe6230016553c3ed,76e1b8690176f801bb,e7409c39013c9db7b4,a5f1519c02b22550e6
83a119ef02346d0879
```

2. [2 балла]. 2-я задача определяется из оставшихся по логину в GitLab. Для определения номера выполните [этот код](/homeworks/get_variant.py), в кач-ве аргумента подайте свой **логин в GitLab.atp-fivt.org**. 

Пример рассчёта номера:
```
$ ./get_variant.py velkerr
302
```

Её нужно решить 3мя способами:
 - на Hadoop (Java API или Streaming)
 - на Hive
 - на Spark

Помимо решения нужно провести исследование, результатом которого станет таблица:

|Способ решения|Кол-во строк кода|cummulative CPU time|
|:--|:--|:--|
|Hadoop|||
|Hive|||
|Spark|||

Каждый способ решения загружаем в отдельную ветку (comparisontask2 - comparisontask5), а результаты сравнения - в comparisontask6.

* В качестве кол-ва строк, считаем [логические строки](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D0%BB%D0%B8%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%BE_%D1%81%D1%82%D1%80%D0%BE%D0%BA_%D0%BA%D0%BE%D0%B4%D0%B0). Если для подсчета строк пишете программу, тоже коммитьте её в репозиторий.
* cummulative CPU time можно посмотреть в метриках YARN. При оценке cummulative CPU time запускайте программу 3 раза. time может незначительно меняться, в таком случае следует брать среднее.

## Как сдать задание

1. Весь код нужно закоммитить не в мастер, а ветку (она для вас уже создана). Когда готовы сдавать - сделайте merge request (все настройки по умолчанию) в мастер.
2. В Git-репозитории каждый способ решения положите в свою папку, а результаты исследования можно отобразить в README.md в корне проекта.

### Задачи

#### 1я задача [PD-2018-20]

**1.(110)** Список идентификаторов перемешайте в случайном порядке. Далее в каждой строке запишите через запятую случайное число идентификаторов - от 1 до 5.
Вариант способа перемешивания записей: дописать к каждой случайное число, отсортировать по нему весь список, потом число отбросить.
* Входные данные: список идентификаторов.
* Формат вывода: id1,id2,...
* Вывод на печать: первые 50 строк.

#### 2я задача

**1 (301).** По данным посещений пользователями страниц посчитайте медиану и [3й квартиль](https://ru.wikipedia.org/wiki/%D0%9A%D0%B2%D0%B0%D0%BD%D1%82%D0%B8%D0%BB%D1%8C#%D0%9C%D0%B5%D0%B4%D0%B8%D0%B0%D0%BD%D0%B0_%D0%B8_%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D0%BB%D0%B8) времен, проведенных на страницах на каждый домен (префикс `www.` нужно отрезать). Результат отсортируйте по медиане, в порядке убывания.
Формулу для эмпирических распределений [отсюда](https://ru.wikipedia.org/wiki/%D0%9A%D0%B2%D0%B0%D0%BD%D1%82%D0%B8%D0%BB%D1%8C).

* Входные данные: посещения страниц пользователями user_events.
* Формат вывода: `domain <tab> median <tab> 3th quartile`
* Вывод на печать: топ 10 строк.

Пример:
```
youtube.com 10 20
```
Это означает, что на страницах с домена youtube.com половина визитов были не длиннее 10 секунд и 75% визитов - не длиннее 20 секунд.

**2 (302).** Посчитайте рейтинг посещений доменов в будние дни с 07:00 до 18:59 ("рабочее" время) и с 19:00 до 06:59 ("нерабочее"). При этом выходные исключаются полностью, т.е. вечер пятницы длится до 23:59. Результат в виде одного файла с тремя полями: домен, рейтинг по посещениям в рабочее, рейтинг по посещениям в нерабочее время. У домена отрезайте `www.` Сортировка по второму полю в порядке убывания.

* Входные данные: посещения страниц пользователями user_events.
* Формат вывода: `domain <tab> work visits rating <tab> free-time visits rating`
* Вывод на печать: топ 10 строк.

Пример:
```
lenta.ru 5 10
```
Это значит, что lenta.ru в рабочее время на 5м месте по посещениям, а в остальное время - на 10м.

**3 (303).** То же, что задача 2, только будни против выходных.

* Входные данные: посещения страниц пользователями user_events.
* Формат вывода: `domain <tab> weekdays visits rating <tab> day-off visits rating`
* Вывод на печать: топ 10 строк.

**4 (304).** Для каждого url посчитайте отношение числа визитов на url к количеству уникальных пользователей на данном url (т.е. visits per user).

Для отсечения статистически не значимых результатов (где один пользователь сделал только 10 заходов) будем использовать сглаживание: в числитель и знаменатель добавлять слагаемое, т.е.

visits per user = (visits + A) / (users + B),

где A и B берутся эмпирически. Возьмите A = 100 и B = 10 (т.е. для страниц, у которых посещаемость ниже 100 визитов и 10 пользователей, результат будет занижен).

Итог отсортируйте по visits per user в порядке убывания.

(Смысл показателя число визитов на пользователя - возвращаемость пользователя на страницу. Для главных страниц порталов, новостных ресурсов, vk.com/feed этот показатель должен быть высоким. Для аналитических, художественных, научно-популярных, новостных статей - близким к единице: один раз прочитал и достаточно.)

* Входные данные: посещения страниц пользователями user_events.
* Формат вывода: `url <tab> visits per user`
* Вывод на печать: топ 10 строк.
