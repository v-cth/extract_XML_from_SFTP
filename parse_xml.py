import pandas as pd
import xml.etree.ElementTree as ET


#Find node and convert content to text
def getElement(Node,ToFind):
    elm = Node.find(ToFind)
    if elm is not None:
        elm = elm.text
    else:
        elm = ''
    return elm

#Flattend xml in table
def parseXml(xml_to_process):  

    data = []
    if xml_to_process is not None:
        for row in xml_to_process:
            file_name = row[0]
            xml_content = row[1]
            file_updated_at = row[2]
            print(file_name)

            # Load XML content from string
            root = ET.fromstring(xml_content)

            # File elements
            sample_code = getElement(root,'SampleCode')
            sample_description = getElement(root,'SampleDescription')
            recipient_lab_code = getElement(root,'RecipientLabCode') 
            reception_date = getElement(root,'ReceptionDate')

            # Navigate in XML file and extract 
            for fraction in root.iter('Fraction'):

                for test in fraction.iter('Test'):
                    test_ref = getElement(test,'TestReference')
                    test_code = getElement(test,'TestCode')

                    for parameter in test.iter('Parameter'):
                        parameter_code = getElement(parameter,'ParameterCode')

                        for result in parameter.iter('Result'):
                            result_value = getElement(result,'ResultValue')
                            result_unit = getElement(result,'ResultUnit')

                            # Append data to a list
                            data.append([
                                file_name,

                                sample_code,
                                sample_description,
                                recipient_lab_code,
                                reception_date,
               
                                test_ref,
                                test_code,

                                parameter_code,

                                result_value,
                                result_unit,
                                file_updated_at
                            ])

                            # Display extracted values
                            print("FileName:", file_name)
                            print("FileUpdatedAt:", file_updated_at)
                            print("SampleCode:", sample_code)
                            print("SampleDescription:", sample_description)
                            print("RecipientLabCode", recipient_lab_code)
                            print("ReceptionDate:", reception_date)
                            print("TestReference:", test_ref)
                            print("TestCode:", test_code)
                            print("ParameterCode:", parameter_code)
                            print("ResultValue:", result_value)
                            print("ResultUnit:", result_unit)
                            print("---")

    # Create dataframe
    result_table = pd.DataFrame(data)
    result_table.columns = [
        "FileName",
        "SampleCode",
        "SampleDescription",
        "RecipientLabCode",
        "ReceptionDate",
        "TestReference",
        "TestCode",
        "ParameterCode",
        "ResultValue",
        "ResultUnit",
        "FileUpdatedAt"
    ]


    return result_table




