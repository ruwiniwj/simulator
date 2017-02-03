/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.event.simulator.constants;

/**
 * Constants related to random data generator
 */
public class RandomDataGeneratorConstants {

    public static final String PRIMITIVE_BASED_ATTRIBUTE = "PRIMITIVEBASED";
    public static final String PROPERTY_BASED_ATTRIBUTE = "PROPERTYBASED";
    public static final String REGEX_BASED_ATTRIBUTE = "REGEXBASED";
    public static final String CUSTOM_DATA_BASED_ATTRIBUTE = "CUSTOMDATA";

    //constants for Property based data generator
    public static final String MODULE_CALENDAR = "Calendar";
    public static final String MODULE_CONTACT = "Contact";
    public static final String MODULE_FINANCE = "Finance";
    public static final String MODULE_INTERNET = "Internet";
    public static final String MODULE_LOCATION = "Location";
    public static final String MODULE_WORDS = "Words";


//constants for each category/module

    //CALENDAR
    public static final String MODULE_CALENDAR_TIME_12_H = "Time12h";
    public static final String MODULE_CALENDAR_TIME_24_H = "Time24h";
    public static final String MODULE_CALENDAR_SECOND = "Second";
    public static final String MODULE_CALENDAR_MINUTE = "Minute";
    public static final String MODULE_CALENDAR_MONTH = "Month";
    public static final String MODULE_CALENDAR_YEAR = "Year";
    public static final String MODULE_CALENDAR_DAY = "Day";
    public static final String MODULE_CALENDAR_DAY_OF_WEEK = "Day of week";
    public static final String MODULE_CALENDAR_MONTH_NUMBER = "Month(Number)";
    public static final String MODULE_CALENDAR_DATE = "Date";

    //CONTACT
    public static final String MODULE_CONTACT_FULL_NAME = "Full Name";
    public static final String MODULE_CONTACT_FIRST_NAME = "First Name";
    public static final String MODULE_CONTACT_LAST_NAME = "last Name";
    public static final String MODULE_CONTACT_ADDRESS = "Address";
    // public static final String Module_contact_birthday="Birthday";
    public static final String MODULE_CONTACT_BSN = "BSN";
    public static final String MODULE_CONTACT_EMAIL = "Email";
    public static final String MODULE_CONTACT_PHONE_NO = "PhoneNo";
    public static final String MODULE_CONTACT_POSTCODE = "PostCode";
    public static final String MODULE_CONTACT_STATE = "State";
    public static final String MODULE_CONTACT_CITY = "City";
    public static final String MODULE_CONTACT_COMPANY = "Company";
    public static final String MODULE_CONTACT_COUNTRY = "Country";
    public static final String MODULE_CONTACT_STREET_NAME = "StreetName";
    public static final String MODULE_CONTACT_HOUSE_NO = "HouseNo";
    public static final String MODULE_CONTACT_HEIGHT_CM = "Height(cm)";
    public static final String MODULE_CONTACT_HEIGHT_M = "Height(m)";
    public static final String MODULE_CONTACT_WEIGHT = "Weight";
    public static final String MODULE_CONTACT_BLOOD_TYPE = "Blood Type";
    public static final String MODULE_CONTACT_OCCUPATION = "Occupation";
    public static final String MODULE_CONTACT_RELIGION = "Religion";
    public static final String MODULE_CONTACT_ZODIAC = "Zodiac";

    //FINANCE
    public static final String MODULE_FINANCE_IBAN = "iban";
    public static final String MODULE_FINANCE_BIC = "bic";
    public static final String MODULE_FINANCE_VISACREDIT_CARD = "Visa CreditCard";
    public static final String MODULE_FINANCE_PIN_CODE = "PinCode";

    //Internet
    public static final String MODULE_INTERNET_URL_BUILDER = "url";
    public static final String MODULE_INTERNET_IP = "ip";
    public static final String MODULE_INTERNET_IPV_6 = "ipv6";
    public static final String MODULE_INTERNET_MAC_ADDRESS = "Mac Address";
    public static final String MODULE_INTERNET_UUID = "UUID";
    public static final String MODULE_INTERNET_COLOR = "Color";
    public static final String MODULE_INTERNET_TWITTER = "Twitter";
    public static final String MODULE_INTERNET_HASHTAG = "HashTag";
    public static final String MODULE_INTERNET_FACEBOOK_ID = "FacebookId";
    public static final String MODULE_INTERNET_USER_NAME = "User Name";

    //LOCATION
    public static final String MODULE_LOCATION_ALTITUDE = "altitude";
    public static final String MODULE_LOCATION_DEPTH = "Depth";
    public static final String MODULE_LOCATION_COORDINATES = "Coordinates";
    public static final String MODULE_LOCATION_LATITUDE = "Latitude";
    public static final String MODULE_LOCATION_LONGTITUDE = "Longtitude";
    public static final String MODULE_LOCATION_GEO_HASH = "GeoHash";

    //WORDS
    public static final String MODULE_WORDS_WORDS = "words";
    public static final String MODULE_WORDS_PARAGRAPH = "paragraph";
    public static final String MODULE_WORDS_SENTENCE = "Sentence";

}
