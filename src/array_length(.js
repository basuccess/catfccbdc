
'{ "i3 Broadband": { "Holding_Company": "WH i3B Bidco LLC\/WH i3B Topco, LLC", "R": [ ], "B": [ ], "X": [ { "max_Adv_DL_speed": "2000", "max_Adv_UL_speed": "40", "low_latency": "1", "Served_Location": "1065431619,1065431635,1065431637,1065431639,1065431642,1065431643,1065431645,1065431646,1065431647,1065431648,1065431649,1065431650,1065431653,1065431654,1065431655,1065431656,1065431657,1065431659,1065431660,1065431661,1065431663,1065431664,1065431665" } ] }, "Cox Communications": { "Holding_Company": "Cox Communications, Inc.", "R": [ { "max_Adv_DL_speed": "2000", "max_Adv_UL_speed": "100", "low_latency": "1", "Served_Location": "1065431646,1065431665,1527756923" } ], "B": [ ], "X": [ ] } }'


array_length(
    array_distinct(
        string_to_array(
            array_to_string(
                array_foreach(
                    map_avals(from_json("Cable")),
                    array_to_string(
                        array_cat(
                            array_foreach(map_get(@element, 'R'), map_get(@element, 'Served_Location')),
                            array_foreach(map_get(@element, 'B'), map_get(@element, 'Served_Location')),
                            array_foreach(map_get(@element, 'X'), map_get(@element, 'Served_Location'))
                        ),
                        ','
                    )
                ),
                ','
            ),
            ','
        )
    )
)

24

{ "T-Mobile": { "Holding_Company": "T-Mobile USA, Inc.", "R": [ ], "B": [ { "max_Adv_DL_speed": "100", "max_Adv_UL_speed": "20", "low_latency": "1", "Served_Location": "1133128739,1133130769,1133130771" } ], "X": [ ] }, "Verizon": { "Holding_Company": "Verizon Communications Inc.", "R": [ ], "B": [ { "max_Adv_DL_speed": "200", "max_Adv_UL_speed": "30", "low_latency": "1", "Served_Location": "1133130769" }, { "max_Adv_DL_speed": "10", "max_Adv_UL_speed": "1", "low_latency": "1", "Served_Location": "1133128739,1133130771" } ], "X": [ ] }, "AT&T": { "Holding_Company": "AT&T Inc.", "R": [ { "max_Adv_DL_speed": "25", "max_Adv_UL_speed": "3", "low_latency": "1", "Served_Location": "1133130769" } ], "B": [ { "max_Adv_DL_speed": "30", "max_Adv_UL_speed": "3", "low_latency": "1", "Served_Location": "1133130769,1133130771" } ], "X": [ ] } }

array_length(
    array_distinct(
        string_to_array(
            array_to_string(
                array_foreach(
                    map_avals(from_json("LicFWA")),
                    array_to_string(
                        array_cat(
                            array_foreach(map_get(@element, 'R'), map_get(@element, 'Served_Location')),
                            array_foreach(map_get(@element, 'B'), map_get(@element, 'Served_Location')),
                            array_foreach(map_get(@element, 'X'), map_get(@element, 'Served_Location'))
                        ),
                        ','
                    )
                ),
                ','
            ),
            ','
        )
    )
)