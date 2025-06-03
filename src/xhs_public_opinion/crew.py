from crewai import Agent, Crew, Process, Task, LLM
from crewai.project import CrewBase, agent, crew, task
from dotenv import load_dotenv
import os

from xhs_public_opinion.tools import (
    AdvancedBrandAnalyzer,
    # todo， 增加一个视频内容ASR的tool，将视频内容转文字。并将文字内容一并输入给Analyzer
)

# 加载环境变量
load_dotenv()

# If you want to run a snippet of code before or after the crew starts,
# you can use the @before_kickoff and @after_kickoff decorators
# https://docs.crewai.com/concepts/crews#example-crew-class-with-decorators

@CrewBase
class XhsPublicOpinionCrew():
	"""小红书公共舆情分析Crew（简化版本：专注于AI内容分析）"""

	# Learn more about YAML configuration files here:
	# Agents: https://docs.crewai.com/concepts/agents#yaml-configuration-recommended
	# Tasks: https://docs.crewai.com/concepts/tasks#yaml-configuration-recommended
	agents_config = 'config/agents.yaml'
	tasks_config = 'config/tasks.yaml'

	deepseek_r1_lm = LLM(
			model="openrouter/deepseek/deepseek-r1-0528:free",
			base_url="https://openrouter.ai/api/v1",
			api_key=os.environ['OPENROUTER_API_KEY'],
			temperature=0.1,
	)

	gemini_flash_llm = LLM(
			model="openrouter/google/gemini-2.5-flash-preview",
			base_url="https://openrouter.ai/api/v1",
			api_key=os.environ['OPENROUTER_API_KEY'],
			temperature=0.1,
	)

	# If you would like to add tools to your agents, you can learn more about it here:
	# https://docs.crewai.com/concepts/agents#agent-tools
	@agent
	def content_analyst(self) -> Agent:
		"""内容分析师 - 负责内容分析"""
		return Agent(
			config=self.agents_config['content_analyst'],
			llm=self.deepseek_r1_lm,
			tools=[AdvancedBrandAnalyzer()],
			verbose=True
		)

	@task
	def content_analysis_task(self) -> Task:
		"""内容分析任务"""
		return Task(
			config=self.tasks_config['content_analysis_task'],
			agent=self.content_analyst()
		)

	@crew
	def crew(self) -> Crew:
		"""创建并配置crew（简化版本：只包含AI内容分析）"""
		return Crew(
			agents=self.agents,
			tasks=self.tasks,
			process=Process.sequential,
			verbose=True
		)
