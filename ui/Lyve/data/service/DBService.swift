//
//  DBService.swift
//  Lyve
//
//  Created by Josh Leung on 5/26/20.
//  Copyright © 2020 Josh Leung. All rights reserved.
//

import Foundation
import Firebase
import FirebaseDatabase

class DBService {
    
    func getDailyStats() {
        var ref: DatabaseReference!
        ref = Database.database().reference()
                
        ref.child("covid").childByAutoId().observeSingleEvent(of: .value) { (snapshot) in
            let value = snapshot.value as? NSDictionary
            
            let date = value?["date"] as? Int ?? 0
            let state = value?["state"] as? String ?? ""
            let positive = value?["positive"] as? Int ?? 0
            let negative = value?["negative"] as? Int ?? 0
            let hospitalizedIncrease =  value?["hospitalizedIncrease"] as? Int ?? 0
            let positiveIncrease    = value?["positiveIncrease"] as? Int ?? 0
            let negativeIncrease    =  value?["negativeIncrease"] as? Int ?? 0
            let deathIncrease       =  value?["deathIncrease"] as? Int ?? 0
            let totalTestResults     = value?["totalTestResults"] as? Int ?? 0
            let totalTestResultsIncrease     = value?["totalTestResultsIncrease"] as? Int ?? 0
            let recovered               = value?["recovered"] as? Int ?? 0
            let death                    = value?["death"] as? Int ?? 0
            let hospitalized            = value?["hospitalized"] as? Int ?? 0
            let hash                    = value?["hash"] as? String ?? ""
            let lastModified               =  value?["lastModified"] as? String ?? ""
            let dateChecked =  value?["dateChecked"] as? String ?? ""
            
            let data = StateHistorical(Date: date, State: state, Positive: positive, Negative: negative, HospitalizedIncrease: hospitalizedIncrease, PositiveIncrease: positiveIncrease, NegativeIncrease: negativeIncrease, DeathIncrease: deathIncrease, TotalTestResults: totalTestResults, TotalTestResultsIncrease: totalTestResultsIncrease, Pending: 0, HospitalizedCurrently: 0, HospitalizedCumulative: 0, InIcuCurrently: 0, InIcuCumulative: 0, OnVentilatorCurrently: 0, OnVentilatorCumulative: 0, Recovered: recovered, Hash: hash, Hospitalized: hospitalized, Death: death, LastModified: lastModified, DateChecked: dateChecked)
            
        }
        
        
    }
}
