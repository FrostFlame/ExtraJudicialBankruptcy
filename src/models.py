# import datetime
# import enum
# from typing import Optional
#
# from sqlalchemy import ForeignKey
# from sqlalchemy.orm import DeclarativeBase
# from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import mapped_column
#
#
# class Base(DeclarativeBase):
#     pass
#
#
# class Bank(Base):
#     id: Mapped[int] = mapped_column(primary_key=True)
#     name: Mapped[str]
#     bik: Mapped[Optional[int]]
#
#
# class ExtrajudicialBankruptcyMessage(Base):
#     id: Mapped[int] = mapped_column(primary_key=True)
#     number: Mapped[int]
#     type: Mapped[str]
#     publish_date: Mapped[datetime.datetime]
#     banks: Mapped[List]
