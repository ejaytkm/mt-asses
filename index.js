const csv = require('csv-parser')
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fs = require('fs')

const csvHeaders = [
    {id: 'From', title: 'From'},
    {id: 'To', title: 'To'},
    {id: 'Payment', title: 'Payment'},
    {id: 'Phone', title: 'Phone'},
    {id: 'Collect_id', title: 'Collect ID'},
    {id: 'Redeem_id', title: 'Redeem ID'},
    {id: 'CollectedRemaining', title: 'Collect Remaining'},
]

let id = 2 

const results = [];
const redemeedList = [];
const userRedeemData = {};
let overallReport = [];

(async () => {
    // READ CSV - Wait till end
    await new Promise((resolve, reject) => {
        fs.createReadStream('csv_in/sample_1.csv')
        .pipe(csv())
        .on('data', (data) => {
            // #STEP0: GET DATA PUSH TO ARRAY FOR RAW USAGE
            data.CollectedRemaining = parseFloat(data.Collected)
            data.id = id
            results.push(data)
            

            if (parseFloat(data.Redemeed) > 0) {
                redemeedList.push(data)
            }

            if (!userRedeemData[data.Phone]) { // define user keyvalue
                userRedeemData[data.Phone] = []
            }
            userRedeemData[data.Phone].push(data)

            id += 1
        })
        .on('end', () => {
            resolve()
        });
    })

    // #STEP1: FILTER BASED ON REDEEMED 
    for (let i = 0; i < redemeedList.length; i++) {
        const redeemData = redemeedList[i]
        const userPhone = redemeedList[i].Phone;

        let redeemRemaining = parseFloat(redeemData.Redemeed)
        const csvWriter = createCsvWriter({
            path: `csv_out/individual/redeem_${redeemData.Phone}_${redeemData.id}.csv`,
            header: csvHeaders,
        });

        // #STEP2: ITERATE BASE ON ONE USER
        const userDataRedeemList = userRedeemData[userPhone]

        const redeemReport = []
        for (let i2 = 0; i2 < userDataRedeemList.length; i2++) {
            const userCollect = userDataRedeemList[i2]

            if (userCollect.CollectedRemaining == 0)  {
                continue
            }

            // STEP #3 - Generate Report, Rebalance Collect and Redeem 
            redeemRemaining = redeemRemaining - userCollect.CollectedRemaining
            if (redeemRemaining > 0) {
                redeemReport.push({
                    From: userCollect.Store,
                    To: redeemData.Store,
                    Payment: userCollect.CollectedRemaining, 
                    Phone: userPhone,
                    Collect_id: userCollect.id,
                    Redeem_id: redeemData.id,
                    CollectedRemaining: 0,
                })
                userDataRedeemList[i2].CollectedRemaining = 0

            } else {
                redeemReport.push({
                    From: userCollect.Store,
                    To: redeemData.Store,
                    Payment: userCollect.CollectedRemaining - Math.abs(redeemRemaining), 
                    Redeem_id: redeemData.id,
                    Phone: userPhone,
                    Collect_id: userCollect.id,
                    Redeem_id: redeemData.id,
                    CollectedRemaining: Math.abs(redeemRemaining),
                })
                userDataRedeemList[i2].CollectedRemaining = Math.abs(redeemRemaining)
                break // finish looping for this redeem entry
            }
        }

        await csvWriter.writeRecords(redeemReport)   
        overallReport = overallReport.concat(redeemReport)
    }

    // (Q2) #STEP 1: GENERATE AGGREGATED REPORT
    const csvWriter = createCsvWriter({
        path: `csv_out/aggregated_report.csv`,
        header: csvHeaders,
    });
    await csvWriter.writeRecords(overallReport)   

    // (Q2) #STEP 2: GENERATE SUMMARY REPORT 
})()
