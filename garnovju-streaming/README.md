producer.py - код, в который можно вводить данные, стоп слово "exit"

keyword_count.py - основной код, обрабатывающий вход

output_consumer.py - ловит выхлоп от детектора

#### Также можно ввести список слов, которые хочется ловить как аргументы к команде запуска спарка, например:

`spark2-submit keyword_detector.py first_word second_word`
