package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.admin.dao.XxlJobGroupDao;
import com.xxl.job.admin.dao.XxlJobInfoDao;
import com.xxl.job.admin.service.XxlJobService;
import com.xxl.job.core.biz.model.ReturnT;
import org.springframework.http.MediaType;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.Arrays;

/**
 * index controller
 * @author xuxueli 2015-12-19 16:13:16
 */
@RestController
@RequestMapping(value = "openJob", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
public class OpenApiJobController {

	@Resource
	private XxlJobGroupDao xxlJobGroupDao;
	@Resource
	private XxlJobInfoDao xxlJobInfoDao;
	@Resource
	private XxlJobService xxlJobService;
	@Resource
	private XxlJobAdminConfig jobAdminConfig;

	/**
	 * 接口签名校验算法
	 * @param info 签名内容
	 * @return String
	 */
	private  String md516(String info) {
		return DigestUtils.md5DigestAsHex(info.getBytes()).substring(8, 24);
	}
	
	@PermissionLimit(limit = false)
	@PostMapping("/add")
	@ResponseBody
	public ReturnT<String> add(@RequestParam("sign") String sign,
							   @RequestBody XxlJobInfo jobInfo) {
		ReturnT<String> ret1 = illegalRequest(sign);
		if (ret1 != null) {
			return ret1;
		}
		// 查询执行器ID
		XxlJobGroup jobGroup = xxlJobGroupDao.findByAppName(jobInfo.getJobGroupName());
		if (jobGroup == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid")+"({0})"), jobInfo.getJobGroupName()));
		}
		// 设置执行器ID
		jobInfo.setJobGroup(jobGroup.getId());
		return xxlJobService.add(jobInfo);
	}

	/**
	 * 签名校验非法
	 * @param sign 请求签名
	 * @return ReturnT<String>
	 */
	private ReturnT<String> illegalRequest(@RequestParam("sign") String sign) {
		String ret = md516(jobAdminConfig.getAccessToken());
		if (StringUtils.isEmpty(sign) || !ret.equals(sign)) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid") + "({0})"), ret));
		}
		return null;
	}

	@PermissionLimit(limit = false)
	@PostMapping("/update")
	@ResponseBody
	public ReturnT<String> update(@RequestParam("sign") String sign,
								  @RequestBody XxlJobInfo jobInfo) {
		ReturnT<String> ret = illegalRequest(sign);
		if (ret != null) {
			return ret;
		}

		XxlJobInfo xxlJobInfo = xxlJobInfoDao.findByBizJobId(jobInfo.getBizJobId());
		if (xxlJobInfo == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid")+"({0})"),
							String.valueOf(jobInfo.getBizJobId())));
		}
		xxlJobInfo.setJobCron(jobInfo.getJobCron());

		return xxlJobService.update(xxlJobInfo);
	}

	@PermissionLimit(limit = false)
	@GetMapping("/remove")
	@ResponseBody
	public ReturnT<String> remove(@RequestParam("sign") String sign,
								  @RequestParam("bizJobId") long bizJobId) {
		ReturnT<String> ret = illegalRequest(sign);
		if (ret != null) {
			return ret;
		}
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.findByBizJobId(bizJobId);
		if (xxlJobInfo == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid")+"({0})"),
							String.valueOf(bizJobId)));
		}
		return xxlJobService.remove(xxlJobInfo.getId());
	}

	@PermissionLimit(limit = false)
	@GetMapping("/stop")
	@ResponseBody
	public ReturnT<String> pause(@RequestParam("sign") String sign,
								 @RequestParam("bizJobId") long bizJobId) {
		ReturnT<String> ret = illegalRequest(sign);
		if (ret != null) {
			return ret;
		}
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.findByBizJobId(bizJobId);
		if (xxlJobInfo == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid")+"({0})"),
							String.valueOf(bizJobId)));
		}
		return xxlJobService.stop(xxlJobInfo.getId());
	}

	@PermissionLimit(limit = false)
	@GetMapping("/start")
	@ResponseBody
	public ReturnT<String> start(@RequestParam("sign") String sign,
								 @RequestParam("bizJobId") long bizJobId) {
		ReturnT<String> ret = illegalRequest(sign);
		if (ret != null) {
			return ret;
		}
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.findByBizJobId(bizJobId);
		if (xxlJobInfo == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid")+"({0})"),
							String.valueOf(bizJobId)));
		}
		return xxlJobService.start(xxlJobInfo.getId());
	}

	@PermissionLimit(limit = false)
	@GetMapping("/trigger")
	@ResponseBody
	public ReturnT<String> triggerJob(@RequestParam("sign") String sign,
									  @RequestParam("bizJobId") long bizJobId,
									  @RequestParam("executorParam") String executorParam) {
		ReturnT<String> ret = illegalRequest(sign);
		if (ret != null) {
			return ret;
		}
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.findByBizJobId(bizJobId);
		if (xxlJobInfo == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE,
					MessageFormat.format((I18nUtil.getString("system_unvalid")+"({0})"),
							String.valueOf(bizJobId)));
		}
		// force cover job param
		if (executorParam == null) {
			executorParam = "";
		}

		JobTriggerPoolHelper.trigger(xxlJobInfo.getId(), TriggerTypeEnum.MANUAL, -1, null, executorParam);
		return ReturnT.SUCCESS;
	}

}
