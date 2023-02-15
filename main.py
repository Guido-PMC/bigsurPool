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
import time
import pandas as pd
from bs4 import BeautifulSoup
import urllib
from binance.spot import Spot
client = Spot()
global last_run
last_run = round(time.time())
BINANCEKEY = os.environ['BINANCEKEY']
BINANCESECRET = os.environ['BINANCESECRET']


if os.environ['ENVIRONMENT'] == "TEST":
    CREDS_BIGQUERY = '/Users/guigonzalez/Desktop/PMCAutomatismos/bigsurmining-14baacf42c48.json'
    BD = "TEST"
else:
    CREDS_BIGQUERY = '/creds/bigsurmining-14baacf42c48.json'
    BD = "BD1"

print(f"OJO ESTOY EN EL AMBIENTE {os.environ['ENVIRONMENT']}, usando BD: {BD} ")
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

def getUsdValue():
    try:
        url = "https://dolarhoy.com/cotizaciondolarblue"
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html,"html.parser")
        tags = soup.find_all("div",class_="value")
        precios = list()
        for tag in tags:
            precios.append(tag.contents[0])
        buy = float(precios[0].replace("$",""))
        sell = float(precios[1].replace("$",""))
    except Exception as e:
        print(e)
        print("Error tomando datos de dolar blue")
        telegram_message("Error tomando datos de dolarhoy probando en 10 minutos")
        time.sleep(600)
        url = "https://dolarhoy.com/cotizaciondolarblue"
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html,"html.parser")
        tags = soup.find_all("div",class_="value")
        precios = list()
        for tag in tags:
            precios.append(tag.contents[0])
        buy = float(precios[0].replace("$",""))
        sell = float(precios[1].replace("$",""))
    return buy

def arsToBtc(amount):
    btc = getBtcValue()
    usd = getUsdValue()
    return ((amount/usd)/btc)

def getUserWallet(usuariosPool):
    return bigQueryRead(f"SELECT paymentWallet FROM {BD}.usuarios WHERE usuariosPool='{usuariosPool}'").iloc[0].iat[0]

def getUserRevShare(usuariosPool):
    return bigQueryRead(f"SELECT revShare FROM {BD}.usuarios WHERE usuariosPool='{usuariosPool}'").iloc[0].iat[0]

def loadUsersBQ():
    usuariosPoolList = []
    electricityList = []
    paysPostRevShareList = []
    valorKhwList = []
    bigqueryDF = bigQueryRead(f"SELECT usuariosPool,powerDraw,paysPostRevShare,valorKhw FROM {BD}.usuarios ORDER BY id ASC")
    print(bigqueryDF)
    print(bigqueryDF["valorKhw"])
    for usuario,consumo,paysPostRevShare,valorKhw in zip(bigqueryDF["usuariosPool"],bigqueryDF["powerDraw"],bigqueryDF["paysPostRevShare"],bigqueryDF["valorKhw"]):
        usuariosPoolList.append(usuario)
        print(f"Cargado usuario {usuario}")
        electricityList.append(consumo)
        print(f"Cargado consumo {consumo}")
        paysPostRevShareList.append(paysPostRevShare)
        print(f"Cargado paysPostRevShare: {paysPostRevShare}")
        valorKhwList.append(valorKhw)
        print(f"Cargado valor Khw: {valorKhw}")
    return usuariosPoolList, electricityList, paysPostRevShareList, valorKhwList

def updateUserData(btcCommission,paymentAmount,inmatureBalance,usuariosPool):
    bigQueryUpdate(f"UPDATE {BD}.usuarios SET revShare_mtd = COALESCE(revShare_mtd, 0)+{btcCommission}, totalPayed_mtd=COALESCE(totalPayed_mtd, 0)+{paymentAmount}, totalMined_mtd=COALESCE(totalMined_mtd,0)+{inmatureBalance}, inmatureBalance=0 WHERE usuariosPool='{usuariosPool}'")
    print("Actualizados valores de tabla Usuario ✔️")

def updateInmatureBalance(inmatureBalance,usuariosPool):
    bigQueryUpdate(f"UPDATE {BD}.usuarios SET inmatureBalance={inmatureBalance} WHERE usuariosPool='{usuariosPool}'")
    print("No llega pago minimo, actualizado saldo inmaduro ✔️")

def getLastId(database):
    return bigQueryRead(f"SELECT id FROM {database} ORDER BY id DESC").iloc[0].iat[0]

def getPendingPaymentsIds(usuariosPool):
    inmatureIdsDF = bigQueryRead(f"SELECT id FROM `{BD}.gananciasDiarias` WHERE pagado is False and usuariosPool='{usuariosPool}' ORDER BY id DESC")
    inmatureIdsString = ','.join(''.join(str(l[0])) for l in inmatureIdsDF.values.tolist())
    return inmatureIdsString

def updatePendingIdsStatus(lastPayId, inmatureIdsString):
    bigQueryUpdate(f"UPDATE {BD}.gananciasDiarias SET pagado=True, idPago='{lastPayId+1}' WHERE id IN ({inmatureIdsString})")
    print("Actualizados IDs de Ganancias Diarias ✔️")

def getNewGananciasId():
    try:
        lastGananciasId = bigQueryRead(f"SELECT id FROM {BD}.gananciasDiarias ORDER BY id DESC").iloc[0].iat[0]
    except Exception as e:
        lastGananciasId = 0
    print(f"ID ultima transaccion : {lastGananciasId}")
    newId = lastGananciasId+1
    return newId
def moveMTDtoSTD(dianuevoMes):
    if datetime.now().day == dianuevoMes:
        print("Hoy es comienzo de mes.")
        usuariosDF = bigQueryRead(f"SELECT * FROM {BD}.usuarios ORDER BY id DESC")
        for usuariosPool,revShare_mtd,totalMined_mtd,totalPayed_mtd in zip(usuariosDF["usuariosPool"], usuariosDF["revShare_mtd"],usuariosDF["totalMined_mtd"],usuariosDF["totalPayed_mtd"]):
            bigQueryUpdate(f"UPDATE {BD}.usuarios SET revShare_std=COALESCE(revShare_std, 0)+{revShare_mtd}, totalMined_std = COALESCE(totalMined_std, 0)+{totalMined_mtd}, totalPayed_std = COALESCE(totalPayed_std, 0)+{totalPayed_mtd} WHERE usuariosPool = '{usuariosPool}'")
            print("Se pasaron valores del mes al START ✔️")
            bigQueryUpdate(f"UPDATE {BD}.usuarios SET revShare_mtd=0, totalMined_mtd=0, totalPayed_mtd = 0 WHERE usuariosPool = '{usuariosPool}'")
            print("Se actualizaron valores del mes a 0 ✔️")

def updateUserMinedToday(usuariosPool, newId):
    json1 = client.mining_earnings_list(algo="sha256",userName=usuariosPool,pageSize=150)
    try:
        bigqueryDate = bigQueryRead(f"SELECT fecha FROM {BD}.gananciasDiarias WHERE usuariosPool='{usuariosPool}'ORDER BY fecha DESC").iloc[0].iat[0]
    except Exception as e:
        bigqueryDate = datetime.utcfromtimestamp(0)
    for pago in reversed(json1["data"]["accountProfits"]):
        print(f"Fecha ultimo pago {bigqueryDate}")
        binanceDate = datetime.utcfromtimestamp(int(pago['time']/1000))
        print(binanceDate.timestamp(), bigqueryDate.timestamp(), binanceDate.day, bigqueryDate.day, binanceDate.month,bigqueryDate.month)
        if (binanceDate.timestamp() > (bigqueryDate.timestamp()+82800)):
            print("Binance > BD")
            print(pago)
            query = bigQueryUpdate(f"INSERT INTO {BD}.gananciasDiarias(id, usuariosPool, dayHashRate, monto, fecha, moneda, pagado) VALUES({newId},'{usuariosPool}',{pago['dayHashRate']/toTerahash},{pago['profitAmount']},TIMESTAMP_SECONDS({int(pago['time']/1000)}),'{pago['coinName']}', False )")
            print(query)
            newId = newId+1

def sendPayment(lastPayId,usuariosPool,wallet,paymentAmount, valorBTC, txid, revShare, btcCommission, paysPostRevShare,inmatureBalance, billDays, startDay, endDay):
    bigQueryUpdate(f"INSERT INTO {BD}.pagosBTC(id, usuariosPool, wallet, monto, fecha, coin, valorBTC, txid, revShare, btcCommission) VALUES({lastPayId+1},'{usuariosPool}','{wallet}',{paymentAmount},TIMESTAMP_SECONDS({(int(datetime.now().replace(microsecond=0).timestamp()))}), 'BTC', {valorBTC}, '{txid}', {revShare}, {btcCommission})")
    print("Agregado pago a cliente en BD ✔️")
    telegram_message(f"Hacer pago a {usuariosPool}, Monto minado: {inmatureBalance}, revShare ({revShare}): {btcCommission}, neto cliente: {paymentAmount}, Cantidad de dias: {billDays}, Inicio: {datetime.utcfromtimestamp(startDay)}, Fin: {datetime.utcfromtimestamp(endDay)}  ")
    print(f"Minimo alcanzado por usuario {usuariosPool} monto minado {inmatureBalance} se paga con id de pago {lastPayId+1} ")
    try:
        client = Spot(key=BINANCEKEY, secret=BINANCESECRET)
        client.withdraw(coin="BTC", amount=paymentAmount, address=wallet, network="BTC", walletType=1)
        telegram_message(f"Se realizo pago de {paymentAmount} BTC a Wallet: {wallet}, cliente {usuariosPool} ")
    except Exception as e:
        telegram_message(f"Error al realizar pago a cliente {usuariosPool} - Error: {e} - paymentAmount: {paymentAmount} - Wallet: {wallet}")


def saveElectricityBill(electricityBill, moneda, usuariosPool, usdValue, btcValue, revShare, btcCommission, startDay, endDay):
    lastId = getLastId(f"{BD}.cobrosLuz")
    bigQueryUpdate(f"INSERT INTO {BD}.cobrosLuz(id, monto, moneda, fecha, usuariosPool, valorUSD, valorBTC, startDay, endDay) VALUES({lastId+1},{electricityBill},'{moneda}',TIMESTAMP_SECONDS({(int(datetime.now().replace(microsecond=0).timestamp()))}), '{usuariosPool}', {usdValue}, {btcValue}, TIMESTAMP_SECONDS({int(startDay)}), TIMESTAMP_SECONDS({int(endDay)}) )")

def updateGananciasDiarias(usuariosPool, minimumPayout, consumo,paysPostRevShare, valorKhw):
    inmatureBalanceDF = bigQueryRead(f"select * from {BD}.gananciasDiarias WHERE usuariosPool='{usuariosPool}' and pagado is False  order by id desc")
    inmatureBalance = 0
    for monto in inmatureBalanceDF["monto"]:
        inmatureBalance = inmatureBalance + monto
    if inmatureBalance > minimumPayout:
        inmatureBalance = round(inmatureBalance,5)
        lastPayId = getLastId(f"{BD}.pagosBTC")
        updatePendingIdsStatus(lastPayId,getPendingPaymentsIds(usuariosPool))
        electricityBill, billDays, startDay, endDay = calculateElectricityBill(usuariosPool, consumo, valorKhw,paysPostRevShare)
        revShare = getUserRevShare(usuariosPool)
        paymentAmountBruto = round((inmatureBalance-arsToBtc(electricityBill)),5)
        print(f"paymentAmount Bruto: {paymentAmountBruto}")
        paymentAmount = round(paymentAmountBruto * (1-revShare),5)
        print(f"paymentAmount: {paymentAmount}")
        btcCommission = round(paymentAmountBruto * (revShare),5)
        print(f"BTC Commision : {btcCommission}")
        updateUserData(btcCommission,paymentAmount,inmatureBalance,usuariosPool)
        sendPayment(lastPayId+1, usuariosPool, getUserWallet(usuariosPool), paymentAmount, getBtcValue(), "1111", revShare, btcCommission, paysPostRevShare,inmatureBalance, billDays, startDay, endDay )
        saveElectricityBill(arsToBtc(electricityBill), "BTC", usuariosPool, getUsdValue(), getBtcValue(), revShare, btcCommission, startDay, endDay)
    elif inmatureBalance < minimumPayout:
        updateInmatureBalance(inmatureBalance, usuariosPool)
    print(f"Monto total acumulado por cliente {usuariosPool} : {inmatureBalance}")

def zabbix_push(puid, key, value):
    stream = os.popen(f"zabbix_sender -z '54.92.215.92'    -s {puid} -k application.{key} -o {str(value)}")
    output = stream.read()
    print(f"ID: {puid}, key: {key}, value: {value} {output[37:][:23]}")


def calculateElectricityBill(usuariosPool, powerDraw, valorKhw,paysPostRevShare):
    lastBillDF = bigQueryRead(f"SELECT * FROM {BD}.cobrosLuz WHERE usuariosPool = '{usuariosPool}' ORDER BY id DESC LIMIT 1")
    lastBillEndDate = str(pd.to_datetime((lastBillDF["endDay"])).values[0])[:10]
    lastBillEndDateDT = datetime.strptime(lastBillEndDate, "%Y-%m-%d")
    startDay = lastBillEndDateDT.replace(microsecond=0).timestamp()
    endDay = datetime.now().replace(microsecond=0).timestamp()
    if paysPostRevShare == True:
        totalBill = 0
        dias = 0
    elif paysPostRevShare == False:
        try:
            delta = datetime.now() - lastBillEndDateDT
            print(f"Cantidad de dias a cobrar: {delta.days}")
            totalBill = delta.days * powerDraw * 24 * valorKhw / 1000
            print(f"Cantidad de pesos a cobrar: {totalBill}")
            dias = delta.days
        except Exception as e:
            print(e)
            dias = 0
            startDay = 0
            endDay = 0
            totalBill = 0
            print(f"Cantidad de pesos a cobrar: {totalBill}")
    return float(totalBill), dias, startDay, endDay

#VARIABLES
toTerahash = 1000000000000
minimumPayout = 0.01
client = Client(key=KEYBINANCE, secret=SECRETBINANCE)


def job():
    global last_run
    last_run = round(time.time())
    global ping
    ping = 1
    #LEO BASE DE DATOS DE usuarios y me hago una lista
    usuariosPoolList, electricityList, paysPostRevShareList, valorKhwList = loadUsersBQ()
    #CHEQUEO SI ES 1ERO DE MES PARA ACTUALIZAR DATOS
    dianuevoMes=1
    moveMTDtoSTD(dianuevoMes)
    #LEO ULTIMO ID EN BD DE gananciasDiarias
    #Por cada usuario en la BD actualizo lo minado hoy y realizo pagos en caso de ser necesario
    print(usuariosPoolList)
    print(electricityList)
    print(paysPostRevShareList)
    print(valorKhwList)
    for usuariosPool, consumo, paysPostRevShare, valorKhw in zip(usuariosPoolList, electricityList,paysPostRevShareList,valorKhwList):
        newId = getNewGananciasId()
        updateUserMinedToday(usuariosPool, newId)
        updateGananciasDiarias(usuariosPool, 0.01, consumo, paysPostRevShare, valorKhw)

def monitor():
    global last_run
    zabbix_push("bigSurPool", "ping", 1)
    zabbix_push("bigSurPool", "last_run", last_run)
    print("--FIN MONITOR--")
job()

schedule.every(1).day.at("12:00").do(job)
schedule.every(1).minutes.do(monitor)

while True:
    schedule.run_pending()
