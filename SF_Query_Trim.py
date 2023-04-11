import re
sq_file='C:\Temp\SF_Query.txt'
sq_file_2='C:\Temp\SF_Query_final.txt'
with open(sq_file_2,'w') as f1:
    pass
with open(sq_file,'r') as f1, open (sq_file_2,'a+') as f2:
      for line in f1:
          if 'to_char' in line:
              new_line = re.sub('to_char', 'trim(to_char', line)
              new_line1 = re.sub('\) is null|\) IS NULL|\)  IS NULL ', ')) is null ', new_line)
          else:
            new_line1 = line
          if 'to_char' in new_line1 and ') END ' in new_line1:
              new_line2=re.sub('\) END ', ')) END ', new_line1)
          #if 'to_char' in new_line1 and ') END ' not in new_line1 and ') as ' in new_line1:
           #   new_line2=re.sub('\) as ', ')) as ', new_line1)

          else:
              new_line2=new_line1
          if 'to_char' in new_line2 and ') as ' in new_line2:
              new_line2 = re.sub('\) as ', ')) as ', new_line2)
          f2.write(new_line2)



