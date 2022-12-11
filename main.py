from binance.spot import Spot as Client
import gspread
from gspread_dataframe import *
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import schedule
from datetime import datetime
import os
import yfinance as yf
import requests



CREDS_BIGQUERY = '/creds/bigsurmining-14baacf42c48.json'
KEYBINANCE = os.environ['KEYBINANCE']
SECRETBINANCE = os.environ['SECRETBINANCE']
minimumPayout = 0.01

def telegram_message(message):
    headers_telegram = {"Content-Type": "application/x-www-form-urlencoded"}
    endpoint_telegram = "https://api.telegram.org/bot1956376371:AAFgQ8zc6HLwRReXnzdfN7csz_-iEl8E1oY/sendMessage"
    mensaje_telegram = {'chat_id': '-791201780', 'text': 'Problemas en RIG'}
    mensaje_telegram["text"] = message
    response = requests.post(endpoint_telegram, headers=headers_telegram, data=mensaje_telegram).json()
    if (response["ok"] == False):
        print("Voy a esperar xq se bloquio telegram")
        time.sleep(response["parameters"]["retry_after"]+5)
        response = requests.post(endpoint_telegram, headers=headers_telegram, data=mensaje_telegram).json()
    return response

def bigQueryUpdate(query):
    client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS_BIGQUERY)
    bq_response = client.query(query=f'{query}').to_dataframe()
    return bq_response

def bigQueryRead(query):
    client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS_BIGQUERY)
    bq_response = client.query(query=f'{query}').to_dataframe()
    return bq_response

def getBtcValue():
    BTC_Ticker = yf.Ticker("BTC-USD")
    BTC_Data = BTC_Ticker.history(period="1D")
    BTC_Value = BTC_Data['High'].loc[BTC_Data.index[0]]
    return BTC_Value

def getUserWallet(usuariosPool):
    return bigQueryRead(f"SELECT paymentWallet FROM BD1.usuarios WHERE usuariosPool='{usuariosPool}'").iloc[0].iat[0]

def getUserRevShare(usuariosPool):
    return bigQueryRead(f"SELECT revShare FROM BD1.usuarios WHERE usuariosPool='{usuariosPool}'").iloc[0].iat[0]

def loadUsersBQ():
    usuariosPoolList = []
    usuariosDF = bigQueryRead("SELECT usuariosPool FROM BD1.usuarios ORDER BY id ASC")
    for usuario in usuariosDF["usuariosPool"]:
        usuariosPoolList.append(usuario)
        print(f"Cargado usuario {usuario}")
    return usuariosPoolList

def getNewGananciasId():
    try:
        lastGananciasId = bigQueryRead("SELECT id FROM BD1.gananciasDiarias ORDER BY id DESC").iloc[0].iat[0]
    except Exception as e:
        lastGananciasId = 0
    print(f"ID ultima transaccion : {lastGananciasId}")
    newId = lastGananciasId+1
    return newId
def moveMTDtoSTD(dianuevoMes):
    if datetime.now().day == dianuevoMes:
        print("Hoy es comienzo de mes.")
        usuariosDF = bigQueryRead(f"SELECT * FROM BD1.usuarios ORDER BY id DESC")
        for usuariosPool,revShare_mtd,totalMined_mtd,totalPayed_mtd in zip(usuariosDF["usuariosPool"], usuariosDF["revShare_mtd"],usuariosDF["totalMined_mtd"],usuariosDF["totalPayed_mtd"]):
            bigQueryUpdate(f"UPDATE BD1.usuarios SET revShare_std=COALESCE(revShare_std, 0)+{revShare_mtd}, totalMined_std = COALESCE(totalMined_std, 0)+{totalMined_mtd}, totalPayed_std = COALESCE(totalPayed_std, 0)+{totalPayed_mtd} WHERE usuariosPool = '{usuariosPool}'")
            print("Se pasaron valores del mes al START ✔️")
            bigQueryUpdate(f"UPDATE BD1.usuarios SET revShare_mtd=0, totalMined_mtd=0, totalPayed_mtd = 0 WHERE usuariosPool = '{usuariosPool}'")
            print("Se actualizaron valores del mes a 0 ✔️")

def updateUserMinedToday(usuariosPool, newId):
    json1 = client.mining_earnings_list(algo="sha256",userName=usuariosPool)
    try:
        bigqueryDate = bigQueryRead(f"SELECT fecha FROM BD1.gananciasDiarias WHERE usuariosPool='{usuariosPool}'ORDER BY fecha DESC").iloc[0].iat[0]
    except Exception as e:
        bigqueryDate = datetime.utcfromtimestamp(0)
    for pago in reversed(json1["data"]["accountProfits"]):
        print(f"Fecha ultimo pago {bigqueryDate}")
        binanceDate = datetime.utcfromtimestamp(int(pago['time']/1000))
        print(binanceDate.timestamp(), bigqueryDate.timestamp(), binanceDate.day, bigqueryDate.day, binanceDate.month,bigqueryDate.month)
        if (binanceDate.timestamp() > (bigqueryDate.timestamp()+82800)):
            print("Binance > BD")
            print(pago)
            query = bigQueryUpdate(f"INSERT INTO BD1.gananciasDiarias(id, usuariosPool, dayHashRate, monto, fecha, moneda, pagado) VALUES({newId},'{usuariosPool}',{pago['dayHashRate']/toTerahash},{pago['profitAmount']},TIMESTAMP_SECONDS({int(pago['time']/1000)}),'{pago['coinName']}', False )")
            print(query)
            newId = newId+1

def payUsers(usuariosPool, minimumPayout):
    inmatureBalanceDF = bigQueryRead(f"select * from BD1.gananciasDiarias WHERE usuariosPool='{usuariosPool}' and pagado is False  order by id desc")
    inmatureBalance = 0
    for monto in inmatureBalanceDF["monto"]:
        inmatureBalance = inmatureBalance + monto

    if inmatureBalance > minimumPayout:
        lastPayId = bigQueryRead("SELECT id FROM BD1.pagosBTC ORDER BY id DESC").iloc[0].iat[0]
        print(f"Minimo alcanzado por usuario {usuariosPool} se pagara {inmatureBalance} con id de pago {lastPayId+1} ")
        inmatureIdsDF = bigQueryRead(f"SELECT id FROM `BD1.gananciasDiarias` WHERE pagado is False and usuariosPool='{usuariosPool}' ORDER BY id DESC")
        inmatureIdsString = ','.join(''.join(str(l[0])) for l in inmatureIdsDF.values.tolist())
        newPayId = lastPayId+1
        revShare = getUserRevShare(usuariosPool)
        paymentAmount = inmatureBalance*(1-revShare)
        valorBTC = getBtcValue()
        txid = "01123"
        btcCommission = inmatureBalance*revShare
        wallet = getUserWallet(usuariosPool)
        bigQueryUpdate(f"UPDATE BD1.gananciasDiarias SET pagado=True, idPago='{newPayId}' WHERE id IN ({inmatureIdsString})")
        print("Actualizados IDs de Ganancias Diarias ✔️")
        bigQueryUpdate(f"INSERT INTO BD1.pagosBTC(id, usuariosPool, wallet, monto, fecha, coin, valorBTC, txid, revShare, btcCommission) VALUES({lastPayId+1},'{usuariosPool}','{wallet}',{paymentAmount},TIMESTAMP_SECONDS({(int(datetime.now().replace(microsecond=0).timestamp()))}), 'BTC', {valorBTC}, '{txid}', {revShare}, {btcCommission})")
        print("Agregado pago a cliente en BD ✔️")
        bigQueryUpdate(f"UPDATE BD1.usuarios SET revShare_mtd = COALESCE(revShare_mtd, 0)+{btcCommission}, totalPayed_mtd=COALESCE(totalPayed_mtd, 0)+{paymentAmount}, totalMined_mtd=COALESCE(totalMined_mtd,0)+{inmatureBalance} WHERE usuariosPool='{usuariosPool}'")
        print("Actualizados valores de tabla Usuario ✔️")
        telegram_message(f"Hacer pago a {usuariosPool}, Monto minado: {inmatureBalance}, revShare: {btcCommission}, neto cliente: {paymentAmount} ")
    elif inmatureBalance < minimumPayout:
        bigQueryUpdate(f"UPDATE BD1.usuarios SET inmatureBalance={inmatureBalance} WHERE usuariosPool='{usuariosPool}'")
        print("No llega pago minimo, actualizado saldo inmaduro ✔️")
    print(f"Monto total acumulado por cliente {usuariosPool} : {inmatureBalance}")

#VARIABLES
toTerahash = 1000000000000
minimumPayout = 0.01
#client = Client(key=KEYBINANCE, secret=SECRETBINANCE)
client = Client(key='Qu7J7lsjEp6Pnw9fHFV51qH24hnjuDoiv2dHwIaKs008ZJAPCisMzE47ferfqOYM', secret='qaY6EQur8pJabWCRpxkGvie0i2dBJf8WVONi8oybk3TvLQUcH1B96P1Lxty4Wf6n')

def job():
    #LEO BASE DE DATOS DE usuarios y me hago una lista
    usuariosPoolList = loadUsersBQ()
    #CHEQUEO SI ES 1ERO DE MES PARA ACTUALIZAR DATOS
    dianuevoMes=1
    moveMTDtoSTD(dianuevoMes)
    #LEO ULTIMO ID EN BD DE gananciasDiarias
    #Por cada usuario en la BD actualizo lo minado hoy y realizo pagos en caso de ser necesario
    for usuariosPool in usuariosPoolList:
        newId = getNewGananciasId()
        updateUserMinedToday(usuariosPool, newId)
        payUsers(usuariosPool, 0.01)
job()

schedule.every(1).day.at("12:00").do(job)

while True:
    schedule.run_pending()
