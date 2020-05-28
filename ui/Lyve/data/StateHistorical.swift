//
//  model.swift
//  Lyve
//
//  Created by Josh Leung on 5/26/20.
//  Copyright © 2020 Josh Leung. All rights reserved.
//

import Foundation

// Our base model
struct StateHistorical : Codable {
    var Date                     : Int
    var State                    : String
    var Positive                 : Int
    var Negative                 : Int
    var HospitalizedIncrease     : Int
    var PositiveIncrease         : Int
    var NegativeIncrease         : Int
    var DeathIncrease            : Int
    var TotalTestResults         : Int
    var TotalTestResultsIncrease : Int
    var Pending                  : Int
    var HospitalizedCurrently    : Int
    var HospitalizedCumulative   : Int
    var InIcuCurrently           : Int
    var InIcuCumulative          : Int
    var OnVentilatorCurrently    : Int
    var OnVentilatorCumulative   : Int
    var Recovered                : Int
    var Hash                     : String
    var Hospitalized             : Int
    var Death                    : Int
    var LastModified             : String
    var DateChecked              : String
}


