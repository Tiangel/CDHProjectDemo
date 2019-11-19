package com.cloudera.vms.similarity;

import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;
import com.cloudera.vms.similarity.stay_focus.StayFocusCond;
import com.cloudera.vms.similarity.stay_focus.StayFocusCondLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StayFocusComputer {
	/**
	 * 过滤需要纠正的文章
	 * 
	 * @param group
	 * @return
	 */
	public static Collection<SimilarityMember> filterNeedCorrectDocs(SimilarityGroup group) {
		List<StayFocusCond> conds = StayFocusCondLoader.conds();
		Set<SimilarityMember> set = new HashSet<>();
		conds.stream().forEach(c -> {

			boolean contains = group.getMembers().stream().anyMatch(m -> {
				String eventsTag = m.getEventsTag();
				if (StringUtils.isBlank(eventsTag))
					return false;
				String[] eventIds = eventsTag.split(",");
				Stream<String> stream = Arrays.stream(eventIds);
				return stream.anyMatch(id -> StringUtils.equals(c.getId(), id));
			});

			if (!contains)
				return;

			boolean similarity = SimilarityComputer.isSimilarity(c.getHmCode(), group.getMeta());
			if (similarity)
				return;

			group.getMembers().forEach(m -> {
				String eventsTag = m.getEventsTag();
				if (StringUtils.isBlank(eventsTag))
					return;
				String[] eventIds = eventsTag.split(",");
				Stream<String> stream = Arrays.stream(eventIds);
				if (stream.anyMatch(id -> StringUtils.equals(c.getId(), id))) {
					stream = Arrays.stream(eventIds);
					String newEventsTag = stream.filter(id -> StringUtils.equals(c.getId(), id))
							.collect(Collectors.joining(","));
					m.setEventsTag(newEventsTag);
					set.add(m);
				}
			});
		});

		return set;
	}

	public static Collection<SimilarityMember> filterNeedCorrectDocs(List<SimilarityMember> members) {
		List<StayFocusCond> conds = StayFocusCondLoader.conds();

		Set<SimilarityMember> set = new ConcurrentSkipListSet<>();

		members.parallelStream().forEach(m -> {
			if (m.getEventsTag() == null || m.getEventsTag().isEmpty())
				return;
			String mHMCode = m.getHmCode();
			String[] tags = m.getEventsTag().split(",");
			List<String> filteredTags = Arrays.stream(tags).filter(tag -> {

				return conds.stream().noneMatch(c -> {
					String orgId = c.getOrganizationid() + "";
					String cHMCode = c.getHmCode();
					return tag.equals(orgId) ? SimilarityComputer.isSimilarity(mHMCode, cHMCode) ? false : true : false;
				});
			}).collect(Collectors.toList());
			if (filteredTags.size() != tags.length) {
				m.setEventsTag(Strings.join(filteredTags, ','));
				set.add(m);
			}

		});

		return set;
	}
	public static SimilarityMember findNeedCorrectDoc(SimilarityMember m) {
		List<StayFocusCond> conds = StayFocusCondLoader.conds();
		if (m.getEventsTag() == null || m.getEventsTag().isEmpty())
			return null;
		String mHMCode = m.getHmCode();
		String[] tags = m.getEventsTag().split(",");
		List<String> filteredTags = Arrays.stream(tags).filter(tag -> {

			return conds.stream().noneMatch(c -> {
				String orgId = c.getOrganizationid() + "";
				String cHMCode = c.getHmCode();
				return tag.equals(orgId) ? SimilarityComputer.isSimilarity(mHMCode, cHMCode) ? false : true : false;
			});
		}).collect(Collectors.toList());
		if (filteredTags.size() == tags.length) 
			return null;
		
		m.setEventsTag(Strings.join(filteredTags, ','));
		return m;

		
		
	}
}
