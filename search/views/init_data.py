# @Time    : 23/02/20 13:27
# @Author  : fyq
# @File    : init_data.py
# @Software: PyCharm

__author__ = 'fyq'

from copy import copy
from dataclasses import asdict
from typing import List

from flask import Blueprint

from search import db, models, SearchDatasource
from search.entity import CommonResult
from flask import jsonify
from search.entity import DataBaseType
from pyparsing import *

from search.models import SearchSQLGenField, SearchFieldGenPath

init_bp = Blueprint("init_data", __name__)


@init_bp.route(rule="/init_data")
def init_data():
    # 数据库
    db.session.add(models.SearchDatasource(name="环思正式库",
                                           ip="10.103.88.88",
                                           port="1433",
                                           db_name="HSWarpERP",
                                           user_name="pdauser",
                                           password="pda2018@#",
                                           db_type=DataBaseType.SQLSERVER
                                           ))
    db.session.add(models.SearchDatasource(name="环思历史库",
                                           ip="10.103.88.88",
                                           port="1433",
                                           db_name="HSWarpERPHistory",
                                           user_name="pdauser",
                                           password="pda2018@#",
                                           db_type=DataBaseType.SQLSERVER))
    db.session.add(models.SearchDatasource(name="环思历史库4y",
                                           ip="10.103.88.88",
                                           port="1433",
                                           db_name="HSWarpERPHistory4Y",
                                           user_name="pdauser",
                                           password="pda2018@#",
                                           db_type=DataBaseType.SQLSERVER
                                           ))

    # 查询字典
    s = models.Search(name="in_store",
                      display="成品入库")
    db.session.add(s)
    db.session.flush()
    # 查询条件
    db.session.add(models.SearchCondition(name="tStoreInBeginTime",
                                          display="入库时间>=",
                                          datatype="date",
                                          order=1,
                                          search_id=s.id))
    db.session.add(models.SearchCondition(name="tStoreInEndTime",
                                          display="入库时间<=",
                                          datatype="date",
                                          order=1,
                                          search_id=s.id))
    # 字段显示
    # SQL
    db.session.add(models.SearchSQL(exp="""SELECT  
    sCompanyName,
    sProductStage,
    sProductType,
    sdiff15,
    tConfirmTime,
    CASE WHEN sDiff04='' THEN sPlanNo ELSE sDiff04 END sXiMa,
    sPlanNo,
    sQCResult,
    sRemark,
    sStoreName,
    sCustomerName,
    sCustomerNo,
    sStoreInNo,
    sDiff01,
    sCreator,
    sInDtlRemark,
    CASE WHEN iStoreInStatus=1 THEN '审核' ELSE '草稿' END status,
    tStoreInTime,
    sStoreInType,
    sSourceName,
    sOrderNo,
    sContractNo,
    sCustomerOrderNo,
    iOrderNO,
    sBarnd,
    sSalesGroupName,
    sSalesName,
    scomponent,
    sMaterialNoSD,
    sMaterialNameSD,
    sMaterialTypeName,
    sMaterialNo,
    sMaterialName,
    sColorNo,
    sColorName,
    sLocalCardNo,
    sOutCardNo,
    sCardWorkCentreName,
    sBarCode,
    nInQty,
    sUnit,
    nStockQty,
    nInGrossQty,
    nInNetQty,
    sSpecialBillingType,
    sFreeReason,
    nInPkgQty,
    sPkgUnit,
    nInPkgExQty,
    sPkgUnitEx,
    sLocation,
    sdiff20,
    nsdSellcontractDtlQty,
    uGUID,
    usdSellContractDtlGUID,
    ummPurchasePlanDtlGUID,
    sConfirmMan
    FROM dbo.vwmmSTInStore WITH(NOLOCK) 
    where (tStoreInTime >= {condition.tStoreInBeginTime} AND tStoreInTime < {condition.tStoreInEndTime}) AND sStoreInType like '%车间生产入库%' """,
                                    main=1,
                                    order=1,
                                    search_id=s.id))

    db.session.add(models.SearchSQL(exp="select sUserName from smUser where sUserID in {result.sCreator}",
                                    main=0,
                                    order=2,
                                    search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="select sPurchaseUnit,splanNo xsplanNo from mmPurchasePlanDtl F WITH(NOLOCK) left join mmPurchasePlanHdr E with(nolock) on E.uguid=F.ummPurchasePlanHdrguid where F.UGUID in {result.ummPurchasePlanDtlGUID}",
        main=0,
        order=3,
        search_id=s.id))

    db.session.add(models.SearchSQL(exp="select sUserName xxUsername from smUser where sUserID in {result.sConfirmMan}",
                                    main=0,
                                    order=4,
                                    search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="select sContractTypeName,sUnit xsUnit,nPrice,nExchangeRate,sCurrency from vwsdSellContract where uGUID in {result.usdSellContractDtlGUID}",
        main=0,
        order=5,
        search_id=s.id))

    db.session.add(
        models.SearchSQL(exp="select sProductUsage,utmArtRouteHdrGUID from mmMaterialProduct where sMaterialNo in {result.sMaterialNoSD}",
                         main=0,
                         order=6,
                         search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="SELECT sValueGroup FROM dbo.pbDataDictionary WITH(NOLOCK) WHERE sTypeCode='ProductUsage' AND bUsable=1 and sValueCode in {result.sProductUsage}",
        main=0,
        order=7,
        search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="select sArtRouteName from tmArtRouteHdr where uGUID in {result.utmArtRouteHdrGUID}",
        main=0,
        order=8,
        search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="SELECT TOP 1 h.tStoreOutTime htStoreOutTime,kx.sStoreOutType kxsStoreOutType FROM dbo.mmSTOutDtl G WITH(NOLOCK) JOIN dbo.mmSTOutHdr H WITH(NOLOCK) ON g.ummOutHdrGUID=h.uGUID JOIN dbo.mmStoreOutType kx WITH(NOLOCK) ON h.immStoreOutTypeID=kx.iID WHERE g.ummInDtlGUID in {result.uGUID}} ORDER BY h.tStoreOutTime",
        main=0,
        order=9,
        search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="SELECT TOP 1 R.nLossRate RnLossRate,sLossDescription FROM psArrangeRelation R WHERE R.usdSellContractDtlGUID in {result.usdSellContractDtlGUID}",
        main=0,
        order=10,
        search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="select c.sCardNo originCardNo, b.sMaterialLot opencardMaterialLot from dbo.psWorkFlowCard b  (NOLOCK) JOIN dbo.psWorkFlowCard c  (NOLOCK) ON b.uRootpsWorkFlowCardGUID=c.uGUID where b.sCardNo in {result.sLocalCardNo}",
        main=0,
        order=11,
        search_id=s.id))

    db.session.add(models.SearchSQL(
        exp="select c.sDepartMent zrdept, b.sRemark xxReason from dbo.pbCommonFabricListDtl b WITH(NOLOCK) LEFT JOIN dbo.pbCommonFabricListHdr c WITH(NOLOCK) ON b.upbCommonFabricListHdrGUID=c.uGUID where b.sFabricNo in {result.sBarCode}",
        main=0,
        order=12,
        search_id=s.id)
    )

    # 字段
    db.session.add(models.SearchField(
        name="sCompanyName",
        display="公司名称",
        rule="{result.sCompanyName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCompanyName"
    ))

    db.session.add(models.SearchField(
        name="sProductStage",
        display="产品阶段",
        rule="{result.sProductStage}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sProductStage"
    ))

    db.session.add(models.SearchField(
        name="sProductType",
        display="生产阶段",
        rule="{result.sProductType}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sProductType"
    ))

    db.session.add(models.SearchField(
        name="sdiff15",
        display="阶段说明",
        rule="{result.sdiff15}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sdiff15"
    ))

    db.session.add(models.SearchField(
        name="tConfirmTime",
        display="审核时间",
        rule="{result.tConfirmTime}",
        datatype="date",
        order=1,
        search_id=s.id,
        result_fields="tConfirmTime"
    ))

    db.session.add(models.SearchField(
        name="sXiMa",
        display="细码单",
        rule="{result.sXiMa}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sXiMa"
    ))

    db.session.add(models.SearchField(
        name="sPlanNo",
        display="委外合同",
        rule="{result.sPlanNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sPlanNo"
    ))

    db.session.add(models.SearchField(
        name="sQCResult",
        display="装单类型",
        rule="{result.sQCResult}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sQCResult"
    ))

    db.session.add(models.SearchField(
        name="sRemark",
        display="备注",
        rule="{result.sRemark}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sRemark"
    ))

    db.session.add(models.SearchField(
        name="sStoreName",
        display="仓库名称",
        rule="{result.sStoreName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sStoreName"
    ))

    db.session.add(models.SearchField(
        name="sCustomerName",
        display="客户",
        rule="{result.sCustomerName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCustomerName"
    ))

    db.session.add(models.SearchField(
        name="sCustomerNo",
        display="客户编码",
        rule="{result.sCustomerNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCustomerNo"
    ))

    db.session.add(models.SearchField(
        name="sStoreInNo",
        display="入库单号",
        rule="{result.sStoreInNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sStoreInNo"
    ))

    db.session.add(models.SearchField(
        name="sDiff01",
        display="到货单号",
        rule="{result.sDiff01}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sDiff01"
    ))

    db.session.add(models.SearchField(
        name="sCreator",
        display="创建人ID",
        rule="{result.sCreator}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCreator"
    ))

    db.session.add(models.SearchField(
        name="sInDtlRemark",
        display="入库明细备注",
        rule="{result.sInDtlRemark}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sInDtlRemark"
    ))

    db.session.add(models.SearchField(
        name="status",
        display="状态",
        rule="{result.status}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="status"
    ))

    db.session.add(models.SearchField(
        name="tStoreInTime",
        display="入库时间",
        rule="{result.tStoreInTime}",
        datatype="date",
        order=1,
        search_id=s.id,
        result_fields="tStoreInTime"
    ))

    db.session.add(models.SearchField(
        name="sStoreInType",
        display="入库类型",
        rule="{result.sStoreInType}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sStoreInType"
    ))

    db.session.add(models.SearchField(
        name="sSourceName",
        display="来源名称",
        rule="{result.sSourceName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sSourceName"
    ))

    db.session.add(models.SearchField(
        name="sOrderNo",
        display="生产订单号",
        rule="{result.sOrderNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sOrderNo"
    ))

    db.session.add(models.SearchField(
        name="sContractNo",
        display="合同号",
        rule="{result.sContractNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sContractNo"
    ))

    db.session.add(models.SearchField(
        name="sCustomerOrderNo",
        display="客户订单号",
        rule="{result.sCustomerOrderNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCustomerOrderNo"
    ))

    db.session.add(models.SearchField(
        name="iOrderNO",
        display="订单行号",
        rule="{result.iOrderNO}",
        datatype="int",
        order=1,
        search_id=s.id,
        result_fields="iOrderNO"
    ))

    db.session.add(models.SearchField(
        name="sBarnd",
        display="品牌",
        rule="{result.sBarnd}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sBarnd"
    ))

    db.session.add(models.SearchField(
        name="sSalesGroupName",
        display="部门",
        rule="{result.sSalesGroupName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sSalesGroupName"
    ))

    db.session.add(models.SearchField(
        name="sSalesName",
        display="销售员",
        rule="{result.sSalesName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sSalesName"
    ))

    db.session.add(models.SearchField(
        name="scomponent",
        display="包装规格",
        rule="{result.scomponent}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="scomponent"
    ))

    db.session.add(models.SearchField(
        name="sMaterialNoSD",
        display="成品编号",
        rule="{result.sMaterialNoSD}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sMaterialNoSD"
    ))

    db.session.add(models.SearchField(
        name="sMaterialNameSD",
        display="成品名称",
        rule="{result.sMaterialNameSD}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sMaterialNameSD"
    ))

    db.session.add(models.SearchField(
        name="sMaterialTypeName",
        display="产品大类",
        rule="{result.sMaterialTypeName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sMaterialTypeName"
    ))

    db.session.add(models.SearchField(
        name="sMaterialNo",
        display="坯布编号",
        rule="{result.sMaterialNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sMaterialNo"
    ))

    db.session.add(models.SearchField(
        name="sMaterialName",
        display="坯布名称",
        rule="{result.sMaterialName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sMaterialName"
    ))

    db.session.add(models.SearchField(
        name="sColorNo",
        display="色号",
        rule="{result.sColorNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sColorNo"
    ))

    db.session.add(models.SearchField(
        name="sColorName",
        display="色名",
        rule="{result.sColorName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sColorName"
    ))

    db.session.add(models.SearchField(
        name="sLocalCardNo",
        display="本厂卡号",
        rule="{result.sLocalCardNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sLocalCardNo"
    ))

    db.session.add(models.SearchField(
        name="sOutCardNo",
        display="外厂卡号",
        rule="{result.sOutCardNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sOutCardNo"
    ))

    db.session.add(models.SearchField(
        name="sCardWorkCentreName",
        display="生产车间",
        rule="{result.sCardWorkCentreName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCardWorkCentreName"
    ))

    db.session.add(models.SearchField(
        name="sBarCode",
        display="布号",
        rule="{result.sBarCode}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sBarCode"
    ))

    db.session.add(models.SearchField(
        name="nInQty",
        display="入库数量",
        rule="{result.nInQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nInQty"
    ))

    db.session.add(models.SearchField(
        name="sUnit",
        display="入库单位",
        rule="{result.sUnit}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sUnit"
    ))

    db.session.add(models.SearchField(
        name="nStockQty",
        display="库存数量",
        rule="{result.nStockQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nStockQty"
    ))

    db.session.add(models.SearchField(
        name="nInGrossQty",
        display="入库毛量",
        rule="{result.nInGrossQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nInGrossQty"
    ))

    db.session.add(models.SearchField(
        name="nInNetQty",
        display="入库净重",
        rule="{result.nInNetQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nInNetQty"
    ))

    db.session.add(models.SearchField(
        name="sSpecialBillingType",
        display="计价策略",
        rule="{result.sSpecialBillingType}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sSpecialBillingType"
    ))

    db.session.add(models.SearchField(
        name="sFreeReason",
        display="免费原因",
        rule="{result.sFreeReason}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sFreeReason"
    ))

    db.session.add(models.SearchField(
        name="nInPkgQty",
        display="辅助数量",
        rule="{result.nInPkgQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nInPkgQty"
    ))

    db.session.add(models.SearchField(
        name="sPkgUnit",
        display="辅助单位",
        rule="{result.sPkgUnit}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sPkgUnit"
    ))

    db.session.add(models.SearchField(
        name="nInPkgExQty",
        display="扩展数量",
        rule="{result.nInPkgExQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nInPkgExQty"
    ))

    db.session.add(models.SearchField(
        name="sPkgUnitEx",
        display="扩展单位",
        rule="{result.sPkgUnitEx}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sPkgUnitEx"
    ))

    db.session.add(models.SearchField(
        name="sLocation",
        display="货架",
        rule="{result.sLocation}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sLocation"
    ))

    db.session.add(models.SearchField(
        name="sdiff20",
        display="产品类型",
        rule="{result.sdiff20}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sdiff20"
    ))

    db.session.add(models.SearchField(
        name="nsdSellcontractDtlQty",
        display="订单数量",
        rule="{result.nsdSellcontractDtlQty}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nsdSellcontractDtlQty"
    ))

    db.session.add(models.SearchField(
        name="uGUID",
        display="",
        rule="{result.uGUID}",
        datatype="float",
        order=1,
        search_id=s.id,
        visible='0',
        result_fields="uGUID"
    ))

    db.session.add(models.SearchField(
        name="sUserName",
        display="创建人",
        rule="{result.sUserName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sUserName"
    ))

    db.session.add(models.SearchField(
        name="sPurchaseUnit",
        display="采购单位",
        rule="{result.sPurchaseUnit}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sPurchaseUnit"
    ))

    db.session.add(models.SearchField(
        name="xsplanNo",
        display="采购单号",
        rule="{result.xsplanNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="xsplanNo"
    ))

    db.session.add(models.SearchField(
        name="xxUsername",
        display="审核人",
        rule="{result.xxUsername}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="xxUsername"
    ))

    db.session.add(models.SearchField(
        name="sContractTypeName",
        display="订单类型",
        rule="{result.sContractTypeName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sContractTypeName"
    ))

    db.session.add(models.SearchField(
        name="xsUnit",
        display="销售单位",
        rule="{result.xsUnit}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="xsUnit"
    ))

    db.session.add(models.SearchField(
        name="nPrice",
        display="单价",
        rule="{result.nPrice}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nPrice"
    ))

    db.session.add(models.SearchField(
        name="nExchangeRate",
        display="汇率",
        rule="{result.nExchangeRate}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="nExchangeRate"
    ))

    db.session.add(models.SearchField(
        name="sCurrency",
        display="币种",
        rule="{result.sCurrency}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sCurrency"
    ))

    db.session.add(models.SearchField(
        name="sProductUsage",
        display="成品用途",
        rule="{result.sProductUsage}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sProductUsage"
    ))

    db.session.add(models.SearchField(
        name="sValueGroup",
        display="用途大类",
        rule="{result.sValueGroup}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sValueGroup"
    ))

    db.session.add(models.SearchField(
        name="sArtRouteName",
        display="总路线",
        rule="{result.sArtRouteName}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sArtRouteName"
    ))

    db.session.add(models.SearchField(
        name="htStoreOutTime",
        display="出库日期",
        rule="{result.htStoreOutTime}",
        datatype="date",
        order=1,
        search_id=s.id,
        result_fields="htStoreOutTime"
    ))

    db.session.add(models.SearchField(
        name="kxsStoreOutType",
        display="出库类型",
        rule="{result.kxsStoreOutType}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="kxsStoreOutType"
    ))

    db.session.add(models.SearchField(
        name="RnLossRate",
        display="阶梯损耗率",
        rule="{result.RnLossRate}",
        datatype="float",
        order=1,
        search_id=s.id,
        result_fields="RnLossRate"
    ))

    db.session.add(models.SearchField(
        name="sLossDescription",
        display="阶梯损耗编码",
        rule="{result.sLossDescription}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="sLossDescription"
    ))

    db.session.add(models.SearchField(
        name="originCardNo",
        display="原始卡号",
        rule="{result.originCardNo}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="originCardNo"
    ))

    db.session.add(models.SearchField(
        name="opencardMaterialLot",
        display="开卡批次",
        rule="{result.opencardMaterialLot}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="opencardMaterialLot"
    ))

    db.session.add(models.SearchField(
        name="zrdept",
        display="责任部门",
        rule="{result.zrdept}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="zrdept"
    ))

    db.session.add(models.SearchField(
        name="xxReason",
        display="不良次品原因",
        rule="{result.xxReason}",
        datatype="str",
        order=1,
        search_id=s.id,
        result_fields="xxReason"
    ))

    db.session.flush()

    search_sql_list = models.SearchSQL.query.filter_by(search_id=s.id).all()
    search_field_list = models.SearchField.query.filter_by(search_id=s.id).all()

    for search_sql in search_sql_list:
        from_key = Keyword("from", caseless=True)
        t, s, e = list(from_key.scan_string(search_sql.exp, max_matches=1))[0]
        # from 语句
        from_exp = search_sql.exp[s:]

        condition = Combine(
            Suppress(Literal("{")) + Word(alphanums) + Literal(".") + Word(alphanums) + Suppress(Literal("}")))
        o = OneOrMore(Word(alphanums + "+-/|&.'=*()><!%,#" + pyparsing_unicode.alphanums))
        ot = OneOrMore(Optional(o) + condition + Optional(o))

        r_l: List[str] = ot.parse_string(from_exp).as_list()
        search_sql.from_exp = ",".join(r_l)
        c_list = []
        r_list = []

        for c_index, c in enumerate(r_l):
            if c.startswith("result."):
                r, f = c.split(".")
                r_list.append(f)

                ret = models.SearchSqlResult()
                ret.search_sql_id = search_sql.id
                ret.result_field = f
                ret.field_name = r_l[c_index-2]

                db.session.add(ret)

            elif c.startswith("condition."):
                c_list.append(c.replace("condition.", ""))

        search_sql.condition_fields = ",".join(c_list)

        def fff(token):
            ff.append(token[0])

        # 字段
        con: str = search_sql.exp[0: s]
        con = con.strip()
        ff: List[str] = []
        field_key = Group(OneOrMore(Word(alphanums + ".='" + pyparsing_unicode.alphanums))) + Suppress(
                Optional(","))
        field_key.set_parse_action(fff)
        ss = Keyword("select", caseless=True)
        all_key = Keyword("all", caseless=True)
        distinct_key = Keyword("distinct", caseless=True)
        top_key = Keyword("top", caseless=True) + Word(nums)
        select_key = Group(ss + Optional(all_key) + Optional(distinct_key) + Optional(top_key))
        select_key.set_parse_action(fff)
        pc = select_key + OneOrMore(field_key)
        pc.parse_string(con)
        search_sql.select_exp = " ".join(ff[0])
        for o in ff[1:]:
            sqf = SearchSQLGenField()
            sqf.search_sql_id = search_sql.id
            sqf.gen_field = o[-1]
            if len(o) > 1:
                sqf.exp_field = " ".join(o[0:len(o)-1])

            db.session.add(sqf)

        # 提交
    db.session.flush()

    def find(l, fields, node, order):
        for s in fields.split(","):
            for i, search_sql in enumerate(l):
                gen_fields = [sss.gen_field for sss in search_sql.search_sql_gen_fields]
                if s in gen_fields:
                    sf = SearchFieldGenPath()
                    sf.search_sql_id = search_sql.id
                    sf.depend_field = s
                    sf.search_field_id = node.id
                    db.session.add(sf)
                    sf.order = order
                    b = list(copy(l))
                    b.pop(i)
                    find(b, ",".join([rett.result_field for rett in search_sql.result_fields]), node, order + 1)
                    break

    search_sql_list = models.SearchSQL.query.all()
    for sfl in search_field_list:
        find(search_sql_list, sfl.result_fields, sfl, 0)

    db.session.commit()
    return jsonify(CommonResult.success(data=None))
