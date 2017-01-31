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
package org.wso2.carbon.event.simulator.randomdatafeedsimulation.utils;

import com.mifmif.common.regex.Generex;
import fabricator.Alphanumeric;
import fabricator.Calendar;
import fabricator.Contact;
import fabricator.Fabricator;
import fabricator.Finance;
import fabricator.Internet;
import fabricator.Location;
import fabricator.Words;

import java.text.DecimalFormat;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import fabricator.enums.DateFormat;
import org.joda.time.DateTime;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.constants.RandomDataGeneratorConstants;

/**
 * Generates random value for given case
 * It is an utility class
 * Data can be generated in three ways
 * 1. Generate data according to given data type
 * For this it uses Fabricator library
 * 2. Generate meaning full data Eg : Full Name
 * For reference {<databaseFeedSimulation href="https://www.mockaroo.com/">www.mockaroo.com</databaseFeedSimulation>}
 * 3. Generate data according to given regular expression
 * For this it uses generex library
 * 4. Generate data with in given data list
 * <p>
 * <databaseFeedSimulation href="http://biercoff.com/fabricator/">fabricator</databaseFeedSimulation>
 * <databaseFeedSimulation href="https://github.com/azakordonets/fabricator">fabricator - github source </databaseFeedSimulation>
 * <databaseFeedSimulation href="https://github.com/mifmif/Generex">Generex</databaseFeedSimulation>
 */
public class RandomDataGenerator {
    /**
     * Initialize contact to generate contact related data
     */
    private static Contact contact = Fabricator.contact();

    /**
     * Initialize calendar to generate calendar related data
     */
    private static Calendar calendar = Fabricator.calendar();

    /**
     * Initialize Finance to generate finance related data
     */
    private static Finance finance = Fabricator.finance();

    /**
     * Initialize internet to generate internet related data
     */
    private static Internet internet = Fabricator.internet();

    /**
     * Initialize location to generate location related data
     */
    private static Location location = Fabricator.location();

    /**
     * Initialize words to generate words related data
     */
    private static Words words = Fabricator.words();

    /**
     * Initialize Alphanumeric to generate words related data
     */
    private static Alphanumeric alpha = Fabricator.alphaNumeric();

    /**
     * Initialize RandomDataGenerator and make it private
     */
    private RandomDataGenerator() {

    }

    /**
     * Generate data according to given data type. And cast it into relevant data type
     * For this it uses Alphanumeric from fabricator library
     *
     * @param type   attribute data type (String,Integer,Float,Double,Long,Boolean)
     * @param min    Minimum value for numeric values to be generate
     * @param max    Maximum value for numeric values to be generated
     * @param length If attribute type is string length indicates length of the string to be generated
     *               If attribute type is Float or Double length indicates no of Numbers after the decimal point
     * @return Generated value as object
     * <databaseFeedSimulation href="http://biercoff.com/fabricator/">fabricator</databaseFeedSimulation>
     */
    public static Object generatePrimitiveBasedRandomData(String type, Object min, Object max, int length) {
        Object result = null;
        DecimalFormat format = new DecimalFormat();
        switch (type) {
            case "Integer":
                result = alpha.randomInt(Integer.parseInt((String) min), Integer.parseInt((String) max));
                break;
            case "Long":
                result = alpha.randomLong(Long.parseLong((String) min), Long.parseLong((String) max));
                break;
            case "Float":
                format.setMaximumFractionDigits(length);
                //Format value to given no of decimals
                result = Float.parseFloat(format.format(alpha.randomFloat(Float.parseFloat((String) min), Float.parseFloat((String) max))));
                break;
            case "Double":
                format.setMaximumFractionDigits(length);
                //Format value to given no of decimals
                result = Double.parseDouble(format.format(alpha.randomFloat(Float.parseFloat((String) min), Float.parseFloat((String) max))));
                break;
            case "String":
                result = alpha.randomString(length);
                break;
            case "Boolean":
                result = alpha.randomBoolean();
                break;
        }
        return result;
    }

    /**
     * Generate data according to given regular expression.
     * It uses  A Java library called Generex for generating String that match
     * databaseFeedSimulation given regular expression
     *
     * @param pattern Regular expression used to generate data
     * @return Generated value as object
     * @see <databaseFeedSimulation href="https://github.com/mifmif/Generex">Generex</databaseFeedSimulation>
     */
    public static Object generateRegexBasedRandomData(String pattern) {
        Generex generex = new Generex(pattern);
        Object result;
        result = generex.random();
        return result;
    }


    /**
     * Generate meaning full data.
     * For this it uses fabricator library
     *
     * @param categoryType CategoryType
     * @param propertyType PropertyType
     * @return Generated value as object
     * @link <databaseFeedSimulation href="http://biercoff.com/fabricator/">fabricator</databaseFeedSimulation>
     */
    public static Object generatePropertyBasedRandomData(String categoryType, String propertyType) {
        Object result = null;
        switch (categoryType) {
            case RandomDataGeneratorConstants.MODULE_CALENDAR:
                switch (propertyType) {
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_TIME_12_H:
                        result = calendar.time12h();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_TIME_24_H:
                        result = calendar.time24h();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_SECOND:
                        result = calendar.second();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_MINUTE:
                        result = calendar.minute();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_MONTH:
                        result = calendar.month();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_MONTH_NUMBER:
                        result = calendar.month(true);
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_YEAR:
                        result = calendar.year();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_DAY:
                        result = calendar.day();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_DAY_OF_WEEK:
                        result = calendar.dayOfWeek();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CALENDAR_DATE:
                        Random random = new Random();
                        int incrementValue = random.nextInt(10);
                        System.out.println(incrementValue);
                        result = calendar.relativeDate(DateTime.now().plusDays(incrementValue)).asString(DateFormat.dd_MM_yyyy_H_m_s_a);
                        break;
                }
                break;

            case RandomDataGeneratorConstants.MODULE_CONTACT:

                switch (propertyType) {

                    case RandomDataGeneratorConstants.MODULE_CONTACT_FULL_NAME:
                        result = contact.fullName(true, true);
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_FIRST_NAME:
                        result = contact.firstName();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_LAST_NAME:
                        result = contact.lastName();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_BSN:
                        result = contact.bsn();
                        break;

                    case RandomDataGeneratorConstants.MODULE_CONTACT_ADDRESS:
                        result = contact.address();
                        break;

                    case RandomDataGeneratorConstants.MODULE_CONTACT_EMAIL:
                        result = contact.eMail();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_PHONE_NO:
                        result = contact.phoneNumber();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_POSTCODE:
                        result = contact.postcode();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_STATE:
                        result = contact.state();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_CITY:
                        result = contact.city();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_COMPANY:
                        result = contact.company();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_COUNTRY:
                        result = contact.country();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_STREET_NAME:
                        result = contact.streetName();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_HOUSE_NO:
                        result = contact.houseNumber();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_HEIGHT_CM:
                        result = contact.height(true);
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_HEIGHT_M:
                        result = contact.height(false);
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_WEIGHT:
                        result = contact.weight(true);
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_BLOOD_TYPE:
                        result = contact.bloodType();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_OCCUPATION:
                        result = contact.occupation();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_RELIGION:
                        result = contact.religion();
                        break;
                    case RandomDataGeneratorConstants.MODULE_CONTACT_ZODIAC:
                        result = contact.zodiac();
                        break;
                }
                break;

            case RandomDataGeneratorConstants.MODULE_FINANCE:


                switch (propertyType) {
                    case RandomDataGeneratorConstants.MODULE_FINANCE_IBAN:
                        result = finance.iban();
                        break;
                    case RandomDataGeneratorConstants.MODULE_FINANCE_BIC:
                        result = finance.bic();
                        break;
                    case RandomDataGeneratorConstants.MODULE_FINANCE_VISACREDIT_CARD:
                        result = finance.visaCard();
                        break;
                    case RandomDataGeneratorConstants.MODULE_FINANCE_PIN_CODE:
                        result = finance.pinCode();
                        break;
                }
                break;

            case RandomDataGeneratorConstants.MODULE_INTERNET:


                switch (propertyType) {
                    case RandomDataGeneratorConstants.MODULE_INTERNET_URL_BUILDER:
                        result = internet.urlBuilder();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_IP:
                        result = internet.ip();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_IPV_6:
                        result = internet.ipv6();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_MAC_ADDRESS:
                        result = internet.macAddress();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_UUID:
                        result = internet.UUID();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_COLOR:
                        result = internet.color();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_TWITTER:
                        result = internet.twitter();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_HASHTAG:
                        result = internet.hashtag();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_FACEBOOK_ID:
                        result = internet.facebookId();
                        break;
                    case RandomDataGeneratorConstants.MODULE_INTERNET_USER_NAME:
                        result = internet.username();
                        break;
                }
                break;

            case RandomDataGeneratorConstants.MODULE_LOCATION:

                switch (propertyType) {
                    case RandomDataGeneratorConstants.MODULE_LOCATION_ALTITUDE:
                        result = location.altitude();
                        break;
                    case RandomDataGeneratorConstants.MODULE_LOCATION_DEPTH:
                        result = location.depth();
                        break;
                    case RandomDataGeneratorConstants.MODULE_LOCATION_COORDINATES:
                        result = location.coordinates();
                        break;
                    case RandomDataGeneratorConstants.MODULE_LOCATION_LATITUDE:
                        result = location.latitude();
                        break;
                    case RandomDataGeneratorConstants.MODULE_LOCATION_LONGTITUDE:
                        result = location.longitude();
                        break;
                    case RandomDataGeneratorConstants.MODULE_LOCATION_GEO_HASH:
                        result = location.geohash();
                        break;
                }
                break;

            case RandomDataGeneratorConstants.MODULE_WORDS:
                switch (propertyType) {
                    case RandomDataGeneratorConstants.MODULE_WORDS_WORDS:
                        result = words.word();
                        break;
                    case RandomDataGeneratorConstants.MODULE_WORDS_PARAGRAPH:
                        result = words.paragraph();
                        break;
                    case RandomDataGeneratorConstants.MODULE_WORDS_SENTENCE:
                        result = words.sentence();
                        break;
                }
                break;

            default:
                System.out.println("Property type is not available in library");

        }

        return result;
    }

    /**
     * Generate data with in given data list
     * <p>
     * Initialize Random to select random element from array
     *
     * @param customDataList Array of data
     * @return generated data from array
     */
    public static Object generateCustomRandomData(String[] customDataList) {
        Random random = new Random();
        int randomElementSelector = random.nextInt(customDataList.length);
        Object result;
        result = customDataList[randomElementSelector];
        return result;
    }

    /**
     * Validate Regular Expression
     *
     * @param regularExpression regularExpression
     */
    public static void validateRegularExpression(String regularExpression) {
        try {
            Pattern.compile(regularExpression);
        } catch (PatternSyntaxException e) {
            throw new EventSimulationException("Invalid regular expression : " + regularExpression + " Error: " + e.getMessage());
        }

    }


}
