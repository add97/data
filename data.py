from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from prettytable import PrettyTable

uri = 'mongodb://alex_du:klajsdoiuhos@candidate.21.mongolayer.com:11694/habitat'
db_name = 'habitat'

def connect(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]

def dailyOrderProgress(db):
    today = str(datetime.today()).split()[0]
    d = timedelta(days=7)
    lastDay = str(datetime.today() - d).split()[0]

    todayTR = 0
    pastTR = 0
    todayCount = 0
    pastCount = 0
    todayTips = 0
    pastTips = 0
    currentDropVar = 0
    pastDropVar = 0

    for t in db.mastertransactions.find({'company_name': {'$ne': 'TEST VENDOR'}}):
        if t['_id'] != '4rhfSBnbaQw5geGNc' and t['_id'] != 'qLLKN4qdDr25Qq5vg' and t['_id'] != 'vx3CmvS8QoYrYkANM':
            txDay = t['createdAt'].split()[0]
            if txDay == today:
                todayCount += 1
                todayTR += t['vendorCommission'] + t['deliveryFee'] + int(0 if t['chargeFee'] is None else t['chargeFee'])
                todayTips += t['tip']
                currentDropVar += t['dropoffVariation']
            elif txDay == lastDay:
                pastCount += 1
                pastTR += t['vendorCommission'] + t['deliveryFee'] + int(0 if t['chargeFee'] is None else t['chargeFee'])
                pastTips += t['tip']
                pastDropVar = pastDropVar + t['dropoffVariation']

    currentOrderTR = todayTR / int(1 if todayCount is 0 else todayCount)
    pastOrderTR = pastTR / pastCount
    currentAvgTips = todayTips / int(1 if todayCount is 0 else todayCount)
    pastAvgTips = pastTips / pastCount
    currentAvgDropVar = currentDropVar / int(1 if todayCount is 0 else todayCount)
    pastAvgDropVar = pastDropVar / pastCount
    orderProgress = '%+d' % (((int(todayCount) - int(pastCount)) / float(pastCount)) * 100)
    TRProgress = '%+d' % (((todayTR - pastTR) / pastTR) * 100)
    orderTRProgress = '%+d' % (((currentOrderTR - pastOrderTR) / pastOrderTR) * 100)
    tipProgress = '%+d' % (((todayTips - pastTips) / pastTips) * 100)
    tipAvgProgress = '%+d' % (((currentAvgTips - pastAvgTips) / pastAvgTips) * 100)
    dropVarProgress = '%+d' % (((currentAvgDropVar - pastAvgDropVar) / pastAvgDropVar) * 100)

    return [todayCount, pastCount, orderProgress, '%0.2f' % todayTR, '%0.2f' % pastTR, TRProgress, '%0.2f' % currentOrderTR,
            '%0.2f' % pastOrderTR, orderTRProgress, '%0.2f' % todayTips, '%0.2f' % pastTips, tipProgress, '%0.2f' % currentAvgTips,
            '%0.2f' % pastAvgTips, tipAvgProgress, '%0.2f' % currentAvgDropVar, '%0.2f' % pastAvgDropVar, dropVarProgress]

def weeklyOrderProgress(db):
    today = datetime.today()
    currentWeek = db.weeks.find().count()
    lastWeek = db.weeks.find().count()-1
    txLimit = str(today + relativedelta(weeks=-1)).split('-')[2].split()[0]

    thisWeekTR = 0
    lastWeekTR = 0
    thisWeekCount = 0
    lastWeekCount = 0
    thisWeekTips = 0
    lastWeekTips = 0
    currentDropVar = 0
    pastDropVar = 0

    for t in db.mastertransactions.find({'$and': [{'company_name': {'$ne': 'TEST VENDOR'}}, {'$or': [{'week': currentWeek}, {'week': lastWeek}]}]}):
        if t['_id'] != '4rhfSBnbaQw5geGNc' and t['_id'] != 'qLLKN4qdDr25Qq5vg' and t['_id'] != 'vx3CmvS8QoYrYkANM':
            txWeek = t['week']
            txDay = t['createdAt'].split('-')[2].split()[0]
            if txWeek == currentWeek:
                thisWeekCount += 1
                thisWeekTR += t['vendorCommission'] + t['deliveryFee'] + int(0 if t['chargeFee'] is None else t['chargeFee'])
                thisWeekTips += t['tip']
                currentDropVar += t['dropoffVariation']
            elif txWeek == lastWeek and txDay <= txLimit:
                lastWeekCount += 1
                lastWeekTR += t['vendorCommission'] + t['deliveryFee'] + int(0 if t['chargeFee'] is None else t['chargeFee'])
                lastWeekTips += t['tip']
                pastDropVar += t['dropoffVariation']

    currentOrderTR = thisWeekTR / int(1 if thisWeekCount is 0 else thisWeekCount)
    pastOrderTR = lastWeekTR / lastWeekCount
    currentAvgTips = thisWeekTips / int(1 if thisWeekCount is 0 else thisWeekCount)
    pastAvgTips = lastWeekTips / lastWeekCount
    currentAvgDropVar = currentDropVar / int(1 if thisWeekCount is 0 else thisWeekCount)
    pastAvgDropVar = pastDropVar / lastWeekCount
    orderProgress = '%+d' % (((int(thisWeekCount) - int(lastWeekCount)) / float(lastWeekCount)) * 100)
    TRProgress = '%+d' % (((thisWeekTR - lastWeekTR) / lastWeekTR) * 100)
    orderTRProgress = '%+d' % (((currentOrderTR - pastOrderTR) / pastOrderTR) * 100)
    tipProgress = '%+d' % (((thisWeekTips - lastWeekTips) / lastWeekTips) * 100)
    tipAvgProgress = '%+d' % (((currentAvgTips - pastAvgTips) / pastAvgTips) * 100)
    dropVarProgress = '%+d' % (((currentAvgDropVar - pastAvgDropVar) / pastAvgDropVar) * 100)

    return [thisWeekCount, lastWeekCount, orderProgress, '%0.2f' % thisWeekTR, '%0.2f' % lastWeekTR, TRProgress, '%0.2f' % currentOrderTR,
            '%0.2f' % pastOrderTR, orderTRProgress, '%0.2f' % thisWeekTips, '%0.2f' % lastWeekTips, tipProgress, '%0.2f' % currentAvgTips,
            '%0.2f' % pastAvgTips, tipAvgProgress, '%0.2f' % currentAvgDropVar, '%0.2f' % pastAvgDropVar, dropVarProgress]

def monthlyOrderProgress(db):
    today = datetime.today()
    currentMonth = str(today).split('-')[1] + '-' + str(today).split('-')[0]
    lastMonth = str(today + relativedelta(months=-1)).split('-')[1] + '-' + str(today + relativedelta(months=-1)).split('-')[0]
    txLimit = str(today).split('-')[2].split()[0]

    thisMonthTR = 0
    lastMonthTR = 0
    thisMonthCount = 0
    lastMonthCount = 0
    thisMonthTips = 0
    lastMonthTips = 0
    currentDropVar = 0
    pastDropVar = 0

    for t in db.mastertransactions.find({'company_name': {'$ne': 'TEST VENDOR'}}):
        if t['_id'] != '4rhfSBnbaQw5geGNc' and t['_id'] != 'qLLKN4qdDr25Qq5vg' and t['_id'] != 'vx3CmvS8QoYrYkANM':
            txMonth = t['createdAt'].split('-')[1] + '-' + t['createdAt'].split('-')[0]
            txDay = t['createdAt'].split('-')[2]
            if txMonth == currentMonth:
                thisMonthCount += 1
                thisMonthTR += t['vendorCommission'] + t['deliveryFee'] + int(0 if t['chargeFee'] is None else t['chargeFee'])
                thisMonthTips += t['tip']
                currentDropVar += t['dropoffVariation']
            elif txMonth == lastMonth and txDay <= txLimit:
                lastMonthCount += 1
                lastMonthTR += t['vendorCommission'] + t['deliveryFee'] + int(0 if t['chargeFee'] is None else t['chargeFee'])
                lastMonthTips += t['tip']
                pastDropVar += t['dropoffVariation']

    currentOrderTR = thisMonthTR / int(1 if thisMonthCount is 0 else thisMonthCount)
    pastOrderTR = lastMonthTR / lastMonthCount
    currentAvgTips = thisMonthTips / int(1 if thisMonthCount is 0 else thisMonthCount)
    pastAvgTips = lastMonthTips / lastMonthCount
    currentAvgDropVar = currentDropVar / int(1 if thisMonthCount is 0 else thisMonthCount)
    pastAvgDropVar = pastDropVar / lastMonthCount
    orderProgress = '%+d' % (((int(thisMonthCount) - int(lastMonthCount)) / float(lastMonthCount)) * 100)
    TRProgress = '%+d' % (((thisMonthTR - lastMonthTR) / lastMonthTR) * 100)
    orderTRProgress = '%+d' % (((currentOrderTR - pastOrderTR) / pastOrderTR) * 100)
    tipProgress = '%+d' % (((thisMonthTips - lastMonthTips) / lastMonthTips) * 100)
    tipAvgProgress = '%+d' % (((currentAvgTips - pastAvgTips) / pastAvgTips) * 100)
    dropVarProgress = '%+d' % (((currentAvgDropVar - pastAvgDropVar) / pastAvgDropVar) * 100)

    return [thisMonthCount, lastMonthCount, orderProgress, '%0.2f' % thisMonthTR, '%0.2f' % lastMonthTR, TRProgress, '%0.2f' % currentOrderTR,
            '%0.2f' % pastOrderTR, orderTRProgress, '%0.2f' % thisMonthTips, '%0.2f' % lastMonthTips, tipProgress, '%0.2f' % currentAvgTips,
            '%0.2f' % pastAvgTips, tipAvgProgress, '%0.2f' % currentAvgDropVar, '%0.2f' % pastAvgDropVar, dropVarProgress]

def makeTable(db):
    dailyOrderData = dailyOrderProgress(db)
    weeklyOrderData = weeklyOrderProgress(db)
    monthlyOrderData = monthlyOrderProgress(db)

    print '\nHow are we doing?\n'

    calculations = ['Current Orders', 'Past Orders', 'Order % Change', 'Current TR', 'Past TR', 'TR % Change', 'Current TR/Order',
            'Past TR/Order', 'TR/Order % Change', 'Current Tips', 'Past Tips', 'Tips % Change', 'Current Tip AVG', 'Past Tip AVG',
            'Tip AVG % Change']
    table = PrettyTable(['', 'Day', 'Week', 'Month'])
    for i in range(0, len(dailyOrderData)):
        if i == 2 or i == 5 or i == 8 or i == 11 or i ==14:
            table.add_row([calculations[i], dailyOrderData[i] + '%', weeklyOrderData[i] + '%', monthlyOrderData[i] + '%'])
        else:
            table.add_row([calculations[i], dailyOrderData[i], weeklyOrderData[i], monthlyOrderData[i]])

    print table

def main():
    db = connect(uri, db_name)
    makeTable(db)

main()


# {u'mealUser': False, u'totalPrice': 9.72, u'runnerId': u'juuz3F9PB26Yfjqge', u'habitat': u'Temple', u'dropoffVariation': -4.02, u'customerCommission': 0.76, u'platformRevenue': 15.12, u'routeTime': 59, u'missed_by_vendor': False, u'chargeFee': 0, u'braintreeId': u'cwe3n4xz', u'vendorY': 39.979302, u'deliveryFee': 2.99, u'firstOrder': u'', u'vendorX': -75.160958, u'receipt_link': u'https://market.tryhabitat.com/emails/preview/user-receipt/8MWkQTai9R6hmqNnz', u'buyerPhone': u'2673075924', u'createdAt': u'2017-01-29 12:19:29', u'adminAssign': False, u'lastUpdated': u'2017-05-25T18:10:25-04:00', u'DaaS': False, u'companyId': u'FshFK6CsWyoeSeH28', u'promoId': u'', u'promoAmount': 0, u'vendorAddress': u'1539 Cecil B. Moore Ave', u'tip': 1, u'snow': u'0', u'dayRequested': u'2017-01-29 12:19:39', u'tempi': u'64.0', u'runnerName': u'Macey', u'fog': u'0', u'company_name': u"Champ's Diner", u'accepted_by_vendor': False, u'buyerId': u'yFBnoDHJSa3pRiT96', u'sellerId': u'FshFK6CsWyoeSeH28', u'vendorCommission': 1.46, u'routeDistance': 237, u'week': 22, u'DaaSType': None, u'promoName': u'', u'buyerCompletedOrdersToDate': 27, u'vendorRating': 5, u'timeRequested': u'2017-01-29 12:19:39', u'completed': True, u'deliveryAddress': u'1828 N 16th St', u'buyerEmail': u'josh.josephs@temple.edu', u'rain': u'0', u'isAcquisition': False, u'buyerLastName': u'', u'cancelled_by_vendor': False, u'buyerName': u'Josh', u'deliveryY': 39.981459, u'deliveryX': -75.160924, u'mealCredits': 0, u'visi': u'8.0', u'conds': u'Overcast', u'delivery': True, u'cancelled_by_admin': False, u'settledByAdmin': True, u'gender': u'male', u'runnerRating': 5, u'dayCreated': u'2017-01-29 12:19:29', u'mealName': u'', u'wspdi': u'19.6', u'_id': u'8MWkQTai9R6hmqNnz', u'order_number': 77237}
