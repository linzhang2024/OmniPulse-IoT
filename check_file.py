with open(r'd:\workspace\Trae\OmniPulse-IoT\server\main.py', encoding='utf-8') as f:
    lines = f.readlines()
    print(f'Total lines: {len(lines)}')
    print('\nLast 20 lines:')
    for i, line in enumerate(lines[-20:], len(lines)-19):
        print(f'{i}: {line.rstrip()}')
