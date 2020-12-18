import csv
import names
import random
import sys
import datetime
qtd_sh    = sys.argv[1]
def gera_cpf():
    return ('{}.{}.{}-{}'.format(random.randint(100,999),random.randint(100,999),random.randint(100,999),random.randint(10,99)))

def gera_nome_completo(qtd):
    return names.get_full_name()

def gera_valor(qtd):
    uniclas = []
    personalite = []
    private = []
    qtd2 = qtd/3
    for i in range(int(qtd2)):
        uniclas.append(round(random.uniform(10000.5,60000.9),2))
    for i in range(int(qtd2)):
        personalite.append(round(random.uniform(15000.5,80000.9),2))
    for i in range(int(qtd2)):
        private.append(round(random.uniform(20000.5,110000.9),2))
    return uniclas, personalite, private

def gera_csv_envia(qlinhas, cvsname):
    print(str(datetime.datetime.now())+': Gerando massa de testes transacoes enviadas ')
    lista_val = []
    lista_cpf = []
    try:
        header = "NOME,CPF,CLASSE"
        with open(cvsname, 'w') as file:
            csvhandler = csv.writer(file)
            csvhandler.writerow(header.split(','))
            for i in range(qlinhas):
                val = round(random.uniform(10000.5,110000.9),2)
                lista_val.append(val)
                cpf = gera_cpf()
                lista_cpf.append(cpf)
                if(val > 50000 and val < 75000 and val < 100000):
                    classe = 'uniclass'
                elif(val > 75000 and val > 50000 and val < 100000):
                    classe = 'personalite'
                else:
                    classe = 'privete'
                linha = '{},{},{}'.format(
                    names.get_full_name(),
                    cpf,
                    classe
                    )
                csvhandler.writerow(linha.split(','))
        return lista_val, lista_cpf
    except:
        raise
valores, chaves = gera_csv_envia(int(qtd_sh),'/sistema/empresas/itau/files/massa_teste.csv')

def gera_csv_rec(qlinhas, cvsname, valores, chaves):
    print(str(datetime.datetime.now())+': Gerando massa de testes transacoes recebidas ')
    try:
        header = "NOME,CPF,VALOR_RECEBIDO,CHAVE_RECEBIDA"
        with open(cvsname, 'w') as file:
            csvhandler = csv.writer(file)
            csvhandler.writerow(header.split(','))
            for i in range(qlinhas):
                linha = '{},{},{},{}'.format(
                    names.get_full_name(),
                    gera_cpf(),
                    valores[random.randint(0,9)]/random.randint(1,10),
                    chaves[random.randint(0,9)]
                    )
                csvhandler.writerow(linha.split(','))
        return True
    except:
        raise
gera_csv_rec(int(qtd_sh)*2,'/sistema/empresas/itau/files/massa_rec.csv',valores,chaves)
