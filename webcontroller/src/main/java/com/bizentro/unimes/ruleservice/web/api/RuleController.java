package com.bizentro.unimes.ruleservice.web.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.jms.Destination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.bizentro.unimes.common.adaptor.MesAdaptor;
import com.bizentro.unimes.common.message.RuleMessage;
import com.bizentro.unimes.common.util.MesException;
import com.bizentro.unimes.manager.converter.MessageConverter;
import com.bizentro.unimes.manager.dispatch.TaskManager;
import com.bizentro.unimes.ruleservice.common.util.RuleCommon;
import com.bizentro.unimes.ruleservice.rules.ui.model.QueryManage;
import com.github.underscore.lodash.U;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * <PRE>
 * 1.ClassName : RuleController.java
 * 2.FileName  : RuleController.java
 * 3.Package   : com.bizentro.unimes.ruleservice.web
 * 4.Comment   : 
 * 5.Author    : user
 * 6.CreateDate: 2021. 8. 11오후 10:21:00
 * 7.UpdateDate:
 * </PRE>
 *
 * <PRE>
 * <B>Process</B>
 * 1.
 * 2.
 * </PRE>
 */

@RestController
@RequestMapping(value = "/rule/*")
public class RuleController {

	protected Logger logger = LoggerFactory.getLogger(RuleController.class);

	@Resource(name = "taskManager")
	protected TaskManager taskManager;

	@Resource(name = "messageConverter")
	protected MessageConverter messageConverter;

	@Resource(name = "adaptor")
	protected MesAdaptor adaptor;

	@Resource(name = "jmsReceiveTemplate")
	private JmsTemplate jmsReceiveTemplate;

	@Resource(name = "subjectList")
	private Map<String, String> subjectList;

	@Resource(name = "RuleCommon")
	public RuleCommon RuleCommon;

	@Resource(name = "QueryManage")
	private QueryManage QueryManage;

	@RequestMapping(value = "/message", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = { RequestMethod.GET,
			RequestMethod.POST })
	public @ResponseBody String message(@RequestBody String message) throws Exception {

		String xml = U.xmlToJson(message);
//			adaptor.send(this.messageConverter.object2RuleMessage(xml));
		return xml;
	}

	@SuppressWarnings("deprecation")
	@RequestMapping(value = "/messagelist", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, method = {
			RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String list(@RequestBody String list) throws Exception {

		// MessageQueue에서 reply 받는 큐를 선언
		Destination replySubject = jmsReceiveTemplate.getDefaultDestination();

		// @RequestBody Message를 List<Json> 형식으로 변환하기 위한 선언
		JsonParser jsonParser = new JsonParser();
		JsonArray jsonList = (JsonArray) jsonParser.parse(list);
		Gson gson = new GsonBuilder().serializeNulls().create();

		// List<Json> 형식으로 변환하여 Return하기 위한 선언
		List<String> respons = new ArrayList<String>();
		String sendjson = new String();
		String resjson = new String();
		String receiveMsg = new String();

		for (int i = 0; i < jsonList.size(); i++) {
			JsonObject jsonObj = jsonList.get(i).getAsJsonObject();
			
			// ServiceType이 Rule일 때 실행
			if ("Rule".equals(jsonObj.get("Msg").getAsJsonObject().get("ServiceType").getAsString())) {

				// json -> xml로 변환
				String json = jsonObj.toString();
				String xml = U.jsonToXml(json);
				RuleMessage msgObj = this.messageConverter.object2RuleMessage(xml);

				// MQ Queue 설정
				msgObj.getHeader().setDestSubject(subjectList.get("destSubject"));
				msgObj.getHeader().setReplySubject(subjectList.get("replySubject"));

				// Queue에 RuleMessage 전송 후 Rule 실행
				adaptor.send(msgObj);

				// reply Queue에서 보낸 uuid 값인 Message만 가져온다.
				Object receive = jmsReceiveTemplate.receiveSelectedAndConvert(replySubject,
						"MsgClientID = '" + msgObj.getHeader().getSeqNo() + "'");
				receiveMsg = receive.toString();

				// 받은 Xml Message를 Json으로 변환하여 List<Json>에 담는다.
				resjson = U.xmlToJson(receiveMsg);
				respons.add(resjson);
			
				// ServiceType이 Query일 때 실행
			} else if ("Query".equals(jsonObj.get("Msg").getAsJsonObject().get("ServiceType").getAsString())) {

				// json에서 쿼리를 실행할 param 값을 HashMap<String, String>형태로 변환
				JsonObject com = jsonObj.get("Msg").getAsJsonObject().get("SEARCHQUERY").getAsJsonObject()
						.get("QUERY").getAsJsonObject();
				
				if(com == null) {
					throw new MesException("NOT Found Json QUERY DATA");
				}

				Map<String, String> obj = new HashMap<String, String>();
				obj = gson.fromJson(com, obj.getClass());
				

				// queryID를 이용하여 Mybatis를 실행하여 result를 List 값으로 받는다.
				String queryID = jsonObj.get("Msg").getAsJsonObject().get("QueryID").getAsString();
				List<Object> selQuery = this.QueryManage.selectQuery(obj, queryID);
				
				// Return시 처리된 결과만 보여주기 위해 result 값을 json에 추가해주고, 기존 Data는 삭제한다.
				jsonObj.get("Msg").getAsJsonObject().get("SEARCHQUERY").getAsJsonObject().remove("QUERY");
				jsonObj.get("Msg").getAsJsonObject().get("SEARCHQUERY").getAsJsonObject().add("QUERY",
						gson.toJsonTree(selQuery));
				respons.add(jsonObj.toString());
			}

		}
		sendjson = respons.toString();
		return sendjson;
	}

}
