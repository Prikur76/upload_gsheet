# API

## Получение списка водителей

```
'Experience': 0, 
'NameConditionWork': '', 
'ID': 2657, 
'FIO': 'Н****в А****й Иванович', 
'Sex': '', 
'BirthDate': '0001-01-01T00:00:00', 
'PhoneNumber': '+7906*****64', 
'PhoneNumber2': '', 
'Email': '', 
'ActualAddress': '', 
'INN': '', 
'OGRN': '', 
'SNILS': '', 
'PassportSerialNumber': '', 
'PassportDepartmentName': '', 
'PassportIssueDate': '0001-01-01T00:00:00', 
'PassportDepartmentCode': '', 
'PassportRegistrationAddress': '', 
'DriversLicenseSerialNumber': '', 
'DriversLicenseIssueDate': '0001-01-01T00:00:00', 
'DriversLicenseExpiryDate': '0001-01-01T00:00:00', 
'AccountNumber': '', 
'CorrAccount': '', 
'BIK': '', 
'Bank': '', 
'Status': 'Уволен', 
'ExternalCar': False, 
'QIWIWalletCardNumber': '', 
'PaymentRecipient': '', 
'Comment': '', 
'Car': 'Х***НМ***', 
'PaymentType': '', 
'EmploymentDate': '0001-01-01T00:00:00', 
'DismissalDate': '2019-06-18T11:50:27', 
'CarDepartment': 'МОСКВА', 
'DriversLicenseExperienceTotalSince': '0001-01-01T00:00:00', 
'DriverDateCreate': '0001-01-01T00:00:00', 
'DefaultID': 'c8a57b1355cf4e368a0e4461f5fb77df', 
'Marketing': '', 
'PhoneNumberSBP': '', 
'BIKSBP': None, 
'Balance': 0, 
'BeginContract': '0001-01-01T00:00:00', 
'EndContract': '0001-01-01T00:00:00', 
'DateDZ': '0001-01-01T00:00:00', 
'SumDZ': 0, 
'CommentDZ': '', 
'UserNameDZ': '', 
'Deposit': 0, 
'ConsolidBalance': 0, 
'ConsolidBalancePaused': 0, 
'DatePL': '0001-01-01T00:00:00', 
'MetaId': 'eb830c4a-e4f5-4e80-b0b0-5f74053567b7'
```

## Получение списка машин

```
https://taksi.0nalog.com:<XXXX>/<XXXXXXX>/hs/Car/v1/Get - GET запрос, получить список всех ТС в базе
Параметры запроса:
{inn} - ИНН организации
{vin} - VIN ТС
{sts} - СТС
{num} - номер ТС
Пример ответа:
[
{
Number: "ТУ28377",
STSSeries: "7729",
STSNumber: "523038",
Model: 'Hyundai Solaris', 
Activity: true - Признак активности ТС (true - активно, штрафы по нему загружаются, false - штрафы не загружаются)
IcorrectData: false - Гос. номер или СТС заполнены неверно
LastInspectionDate: None,
LastInspectionMileage: None,
INN: "" - ИНН
KPP: "" - KPP
Department: "" - подразделение
OnLine: true - ТС на линии
Status: "Не работает" - статус ТС
Code: "13233" - код ТС
STSIssueDate: "0001-01-01T00:00:00" - дата выдачи СТС
STSValidityDate: "0001-01-01T00:00:00" - СТС срок действия
YearCar: "2020-01-01T00:00:00" - год выпуска ТС
Organization: "ИНТЕЛ" - организация ТС
LandLord: "Рога и копыта" - арендодатель
UsageType: "Штат" - тип использования
Driver: "Иванов Иван" - водитель
BirthDate: "2000-01-01T00:00:00" - дата рождения
SubStatus: "" - подстатус ТС
Reason: "" - причина смены статуса
Comment: "" - комментарий смены статуса
MileAge: 30000 - пробег
DaysFromInspection: 30 - прошло дней с осмотра
KPPType: "" - тип КПП
LastDateOfProtocol: "2020-02-02T00:00:00" - дата посл. протокола
Gas: true - есть ГБО
Brand: "" - брендирование
VIN: "" - вин
OSAGOSeries: "" - осаго, серия
OSAGONumber: "" - осаго, номер
OSAGOIssueDate: "0001-01-01T00:00:00" - осаго, дата выдачи
OSAGOValidityDate: "0001-01-01T00:00:00" - осаго, срок действия
OSAGOBonus: 500 - осаго, премия
OSAGOInsurer: "Иванов И" - осаго, страховщик
LicenseSeriesNumber: "" - лицензия такси, серия и номер
LicenseIssueDate: "0001-01-01T00:00:00" - лицензия такси, дата выдачи
LicenseValidityDate: "0001-01-01T00:00:00" - лицензия такси, срок действия
LicenseLicensee: "" - лицензия такси, лицензиат
Region: "" - регион
DisableDocumentStatus: "" - исключить из документа статусы ТС
DisableContract: "" - исключить из договоров
TOSeriesNumber: "" - тех. осмотр серия, номер
TOIssueDate: "0001-01-01T00:00:00" - дата выдачи тех. осмотра
TOValidityDate: "0001-01-01T00:00:00" - срок действия тех.осмотра
}
]
```

