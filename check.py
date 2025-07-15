def check_duplicates(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        wallets = [line.strip() for line in file if line.strip()]

    total = len(wallets)
    unique_wallets = set(wallets)
    duplicates_count = total - len(unique_wallets)

    print(f"Всего кошельков: {total}")
    print(f"Уникальных: {len(unique_wallets)}")
    print(f"Дубликатов: {duplicates_count}")

# Пример использования:
check_duplicates("ready.txt")