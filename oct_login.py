import oct_config
with open(oct_config.config_path + 'SFP_P.txt', 'r') as uf2:
    sfp = uf2.readline().rstrip('\n')
with open(oct_config.config_path + 'UID_P.txt', 'r') as uf3:
    myp = uf3.readline().rstrip('\n')